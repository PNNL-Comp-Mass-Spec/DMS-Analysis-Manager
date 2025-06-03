using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;
using System.Xml.Linq;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.FileAndDirectoryTools;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.OfflineJobs;
using AnalysisManagerBase.StatusReporting;
using MyEMSLReader;
using ParamFileGenerator;
using PHRPReader;
using PRISM;
using PRISM.Logging;
using PRISMDatabaseUtils;
using Renci.SshNet.Sftp;

//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 12/19/2007
//*********************************************************************************************************

// ReSharper disable UnusedMember.Global

namespace AnalysisManagerBase.AnalysisTool
{
    /// <summary>
    /// Base class for job resource class
    /// </summary>
    public abstract class AnalysisResources : AnalysisMgrBase, IAnalysisResources
    {
        // Ignore Spelling: acqu, baf, cdf, cdta, centroided, Cn, deconv, dta, Fasta, fht, fid, Formularity, gz, gzip, hashcheck, histone, loc
        // Ignore Spelling: maldi, MaxQuant, mgf, MODa, msalign, MSFragger, msgf, msgfdb, msgfplus, MSPathFinder, msxml, myemsl, mzml, mzxml, na, num
        // Ignore Spelling: parm, pbf, protoapps, psm, ReporterIons, resourcer, sequest, ser, tdf, tims, tof, tsv, uimf, unpurged, utils, wiff, xt, xtandem
        // Ignore Spelling: Bruker, Micromass, Orbitrap

        /// <summary>
        /// Dataset name for aggregation jobs
        /// </summary>
        public const string AGGREGATION_JOB_DATASET = "Aggregation";

        /// <summary>
        /// MyEMSL path flag
        /// </summary>
        public const string MYEMSL_PATH_FLAG = MyEMSLUtilities.MYEMSL_PATH_FLAG;

        // Note: Each RAW_DATA_TYPE constant needs to be all lowercase

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
        /// Thermo IonTrap/LTQ-FT/Orbitrap data
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

        // ReSharper disable GrammarMistakeInComment

        /// <summary>
        /// Bruker_FT directory data
        /// </summary>
        /// <remarks>
        /// 12T datasets acquired prior to 7/16/2010 use a Bruker data station and have an analysis.baf file, 0.ser directory,
        /// and a XMASS_Method.m subdirectory with file apexAcquisition.method
        /// - Datasets will have an instrument name of 12T_FTICR and raw_data_type of "zipped_s_folders"
        ///   12T datasets acquired after 9/1/2010 use the Agilent data station, and thus have a .D directory
        /// - Datasets will have an instrument name of 12T_FTICR_B and raw_data_type of "bruker_ft"
        ///   15T datasets also have raw_data_type "bruker_ft"
        /// - Inside the .D directory is the analysis.baf file; there is also .m subdirectory that has a apexAcquisition.method file
        /// </remarks>
        public const string RAW_DATA_TYPE_BRUKER_FT_FOLDER = "bruker_ft";

        /// <summary>
        /// Bruker MALDI spot data
        /// </summary>
        /// <remarks>
        /// This is used by BrukerTOF_01 (e.g. Bruker TOF_TOF)
        /// Directory has a .EMF file and a single subdirectory that has an acqu file and fid file
        /// </remarks>
        public const string RAW_DATA_TYPE_BRUKER_MALDI_SPOT = "bruker_maldi_spot";

        /// <summary>
        /// Bruker MALDI imaging data
        /// </summary>
        /// <remarks>
        /// This is used by instruments 9T_FTICR_Imaging and BrukerTOF_Imaging_01
        /// Series of zipped subdirectories, with names like 0_R00X329.zip; subdirectories inside the .Zip files have fid files
        /// </remarks>
        public const string RAW_DATA_TYPE_BRUKER_MALDI_IMAGING = "bruker_maldi_imaging";

        /// <summary>
        /// Bruker TOF baf data
        /// </summary>
        /// <remarks>
        /// This is used by instrument Maxis_01
        /// Inside the .D directory is the analysis.baf file; there is also .m subdirectory that has a microTOFQMaxAcquisition.method file;
        /// there is not a ser or fid file
        /// </remarks>
        public const string RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER = "bruker_tof_baf";

        // ReSharper disable CommentTypo

        /// <summary>
        /// Bruker TOF tdf data
        /// </summary>
        /// <remarks>
        /// This is used by instrument External_Bruker_timsTOF
        /// Inside the .D directory are files analysis.tdf and analysis.tdf_bin; there is also .m subdirectory that has a microTOFQImpacTemAcquisition.method file;
        /// there is not a ser or fid file
        /// </remarks>
        public const string RAW_DATA_TYPE_BRUKER_TOF_TDF_FOLDER = "bruker_tof_tdf";

        // ReSharper restore GrammarMistakeInComment

        // ReSharper restore CommentTypo

        /// <summary>
        /// Result type for DIA-NN
        /// </summary>
        public const string RESULT_TYPE_DIANN = "DNN_Peptide_Hit";

        /// <summary>
        /// Result type for Inspect
        /// </summary>
        public const string RESULT_TYPE_INSPECT = "IN_Peptide_Hit";

        /// <summary>
        /// Result type for MaxQuant
        /// </summary>
        public const string RESULT_TYPE_MAXQUANT = "MXQ_Peptide_Hit";

        /// <summary>
        /// Result type for MODa
        /// </summary>
        public const string RESULT_TYPE_MODA = "MODa_Peptide_Hit";

        /// <summary>
        /// Result type for ModPlus
        /// </summary>
        public const string RESULT_TYPE_MODPLUS = "MODPlus_Peptide_Hit";

        /// <summary>
        /// Result type for MSAlign
        /// </summary>
        public const string RESULT_TYPE_MSALIGN = "MSA_Peptide_Hit";

        /// <summary>
        /// Result type for MSFragger (including FragPipe jobs)
        /// </summary>
        public const string RESULT_TYPE_MSFRAGGER = "MSF_Peptide_Hit";

        /// <summary>
        /// Result type for MS-GF+ (aka MSGF+)
        /// (and previously MSGFDB)
        /// </summary>
        public const string RESULT_TYPE_MSGFPLUS = "MSG_Peptide_Hit";

        /// <summary>
        /// Result type for MSPathfinder
        /// </summary>
        public const string RESULT_TYPE_MSPATHFINDER = "MSP_Peptide_Hit";

        /// <summary>
        /// Result type for SEQUEST
        /// </summary>
        public const string RESULT_TYPE_SEQUEST = "Peptide_Hit";

        /// <summary>
        /// Result type for TopPIC
        /// </summary>
        public const string RESULT_TYPE_TOPPIC = "TPC_Peptide_Hit";

        /// <summary>
        /// Result type for X!Tandem
        /// </summary>
        public const string RESULT_TYPE_XTANDEM = "XT_Peptide_Hit";

        /// <summary>
        /// Concatenated dta file
        /// </summary>
        public const string CDTA_EXTENSION = "_dta.txt";

        /// <summary>
        /// Concatenated dta file
        /// </summary>
        public const string CDTA_ZIPPED_EXTENSION = "_dta.zip";

        /// <summary>
        /// Zipped .mgf file
        /// </summary>
        public const string MGF_ZIPPED_EXTENSION = "_mgf.zip";

        /// <summary>
        /// QStar .wiff file
        /// </summary>
        public const string DOT_WIFF_EXTENSION = ".wiff";

        /// <summary>
        /// .d file (or .d directory)
        /// </summary>
        public const string DOT_D_EXTENSION = ".d";

        /// <summary>
        /// .raw file (or .raw directory)
        /// </summary>
        public const string DOT_RAW_EXTENSION = ".raw";

        /// <summary>
        /// .uimf file
        /// </summary>
        public const string DOT_UIMF_EXTENSION = ".uimf";

        /// <summary>
        /// .gz file
        /// </summary>
        public const string DOT_GZ_EXTENSION = ".gz";

        /// <summary>
        /// .mzXML file
        /// </summary>
        public const string DOT_MZXML_EXTENSION = ".mzXML";

        /// <summary>
        /// .mzML file
        /// </summary>
        public const string DOT_MZML_EXTENSION = ".mzML";

        /// <summary>
        /// .mgf file
        /// </summary>
        public const string DOT_MGF_EXTENSION = ".mgf";

        /// <summary>
        /// .cdf file
        /// </summary>
        public const string DOT_CDF_EXTENSION = ".cdf";

        /// <summary>
        /// .pbf file
        /// </summary>
        public const string DOT_PBF_EXTENSION = ".pbf";

        /// <summary>
        /// Feature file, generated by the ProMex tool
        /// </summary>
        public const string DOT_MS1FT_EXTENSION = ".ms1ft";

        /// <summary>
        /// FASTA file extension
        /// </summary>
        protected const string FASTA_FILE_EXTENSION = ".fasta";

        private const string LOCALHASHCHECK_EXTENSION = ".localhashcheck";

        /// <summary>
        /// Storage path info file suffix
        /// </summary>
        public const string STORAGE_PATH_INFO_FILE_SUFFIX = FileCopyUtilities.STORAGE_PATH_INFO_FILE_SUFFIX;

        /// <summary>
        /// Scan stats file suffix: _ScanStats.txt
        /// </summary>
        public const string SCAN_STATS_FILE_SUFFIX = ReaderFactory.SCAN_STATS_FILENAME_SUFFIX;

        /// <summary>
        /// Extended scan stats file suffix: _ScanStatsEx.txt
        /// </summary>
        public const string SCAN_STATS_EX_FILE_SUFFIX = ReaderFactory.EXTENDED_SCAN_STATS_FILENAME_SUFFIX;

        /// <summary>
        /// SIC stats file suffix
        /// </summary>
        public const string SIC_STATS_FILE_SUFFIX = "_SICStats.txt";

        /// <summary>
        /// ReporterIons file suffix
        /// </summary>
        public const string REPORTERIONS_FILE_SUFFIX = "_ReporterIons.txt";

        /// <summary>
        /// Data package SpectraFile suffix
        /// </summary>
        public const string DATA_PACKAGE_SPECTRA_FILE_SUFFIX = "_SpectraFile";

        /// <summary>
        /// Bruker 0.ser directory
        /// </summary>
        public const string BRUKER_ZERO_SER_FOLDER = "0.ser";

        /// <summary>
        /// Bruker ser file name
        /// </summary>
        public const string BRUKER_SER_FILE = "ser";

        /// <summary>
        /// Bruker fid file name
        /// </summary>
        public const string BRUKER_FID_FILE = "fid";

        /// <summary>
        /// Cache folder root path
        /// </summary>
        /// <remarks>
        /// Default values:
        ///   DIA-NN:          \\proto-9\DiaNN_Staging
        ///   MaxQuant:        \\protoapps\MaxQuant_Staging
        ///   FragPipe:        \\proto-9\MSFragger_Staging
        ///   MSFragger:       \\proto-9\MSFragger_Staging
        ///   PRIDE_Converter: \\protoapps\MassIVE_Staging
        /// </remarks>
        public const string JOB_PARAM_CACHE_FOLDER_ROOT_PATH = "CacheFolderRootPath";

        /// <summary>
        /// Dataset directory name
        /// </summary>
        public const string JOB_PARAM_DATASET_FOLDER_NAME = "DatasetFolderName";

        /// <summary>
        /// Dataset name
        /// </summary>
        public const string JOB_PARAM_DATASET_NAME = "DatasetName";

        /// <summary>
        /// Legacy dataset name parameter
        /// </summary>
        [Obsolete("Use JOB_PARAM_DATASET_NAME instead")]
        public const string JOB_PARAM_DATASET_NAME_LEGACY = "DatasetNum";

        /// <summary>
        /// Data package path
        /// </summary>
        public const string JOB_PARAM_DATA_PACKAGE_PATH = "DataPackagePath";

        /// <summary>
        /// Packed job parameter DatasetFilePaths
        /// </summary>
        public const string JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS = "PackedParam_DatasetFilePaths";

        /// <summary>
        /// Packed job parameter DatasetRawDataTypes
        /// </summary>
        /// <remarks>This is used by AnalysisResourcesRepoPkgr and AnalysisManagerMsXmlGenPlugIn</remarks>
        public const string JOB_PARAM_DICTIONARY_DATASET_RAW_DATA_TYPES = "PackedParam_DatasetRawDataTypes";

        /// <summary>
        /// Packed job parameter JobDatasetMap
        /// </summary>
        /// <remarks>This is used by AnalysisResourcesPhosphoFdrAggregator</remarks>
        public const string JOB_PARAM_DICTIONARY_JOB_DATASET_MAP = "PackedParam_JobDatasetMap";

        /// <summary>
        /// Packed job parameter JobSettingsFileMap
        /// </summary>
        /// <remarks>This is used by AnalysisResourcesPhosphoFdrAggregator</remarks>
        public const string JOB_PARAM_DICTIONARY_JOB_SETTINGS_FILE_MAP = "PackedParam_JobSettingsFileMap";

        /// <summary>
        /// Packed job parameter JobToolNameMap
        /// </summary>
        /// <remarks>This is used by AnalysisResourcesPhosphoFdrAggregator</remarks>
        public const string JOB_PARAM_DICTIONARY_JOB_TOOL_MAP = "PackedParam_JobToolNameMap";

        /// <summary>
        /// Job parameter specifying the input directory name
        /// </summary>
        public const string JOB_PARAM_INPUT_FOLDER_NAME = "InputFolderName";

        /// <summary>
        /// Job parameter tracking the shared results directory (or comma separated list of directories)
        /// </summary>
        public const string JOB_PARAM_SHARED_RESULTS_FOLDERS = "SharedResultsFolders";

        /// <summary>
        /// Job parameter to track the auto-generated FASTA file name
        /// </summary>
        public const string JOB_PARAM_GENERATED_FASTA_NAME = "GeneratedFastaName";

        /// <summary>
        /// Job parameter of the directory with cached .mzML (and .mzXML) files
        /// </summary>
        public const string JOB_PARAM_MSXML_CACHE_FOLDER_PATH = "MSXMLCacheFolderPath";

        /// <summary>
        /// Output directory name
        /// </summary>
        public const string JOB_PARAM_OUTPUT_FOLDER_NAME = "OutputFolderName";

        /// <summary>
        /// Parameter file name
        /// </summary>
        public const string JOB_PARAM_PARAMETER_FILE = "ParamFileName";

        /// <summary>
        /// Step tool parameter file from the previous job step
        /// </summary>
        public const string JOB_PARAM_PREVIOUS_JOB_STEP_TOOL_PARAMETER_FILE = "PreviousJobStepToolParameterFile";

        /// <summary>
        /// Transfer directory path
        /// </summary>
        public const string JOB_PARAM_TRANSFER_DIRECTORY_PATH = "TransferFolderPath";

        /// <summary>
        /// Name of the XML file with job parameters, created in the working directory
        /// </summary>
        public const string JOB_PARAM_XML_PARAMS_FILE = "GenJobParamsFilename";

        /// <summary>
        /// Manager parameter: local directory for caching FASTA files
        /// Also caches CIA DB files for Formularity
        /// </summary>
        public const string MGR_PARAM_ORG_DB_DIR = "OrgDBDir";

        /// <summary>
        /// Warning message that spectra are not centroided
        /// </summary>
        /// <remarks>This constant is used by AnalysisToolRunnerMSGFDB, AnalysisResourcesMSGFDB, and AnalysisResourcesDtaRefinery</remarks>
        public const string SPECTRA_ARE_NOT_CENTROIDED = "None of the spectra are centroided; unable to process";

        /// <summary>
        /// Zip file with _ion.tsv, _peptide.tsv, _protein.tsv, and _psm.tsv files
        /// </summary>
        public const string ZIPPED_MSFRAGGER_PSM_TSV_FILES = "Dataset_PSM_tsv.zip";

        /// <summary>
        /// Instrument data file type enum
        /// </summary>
        public enum RawDataTypeConstants
        {
            /// <summary>
            /// Unknown data type
            /// </summary>
            Unknown = 0,

            /// <summary>
            /// Thermo .raw file
            /// </summary>
            ThermoRawFile = 1,

            /// <summary>
            /// IMS .UIMF file
            /// </summary>
            UIMF = 2,

            /// <summary>
            /// mzXML file
            /// </summary>
            mzXML = 3,

            /// <summary>
            /// mzML file
            /// </summary>
            mzML = 4,

            /// <summary>
            /// Agilent ion trap data, Agilent TOF data
            /// </summary>
            AgilentDFolder = 5,

            /// <summary>
            /// QStar .wiff file
            /// </summary>
            AgilentQStarWiffFile = 6,

            /// <summary>
            ///  Micromass QTOF data
            /// </summary>
            MicromassRawFolder = 7,

            /// <summary>
            /// FTICR data, including instrument 3T_FTICR, 7T_FTICR, 9T_FTICR, 11T_FTICR, 11T_FTICR_B, and 12T_FTICR
            /// </summary>
            ZippedSFolders = 8,

            // ReSharper disable once GrammarMistakeInComment

            /// <summary>
            /// .D directory is the analysis.baf file; there is also .m subdirectory that has a apexAcquisition.method file
            /// </summary>
            BrukerFTFolder = 9,

            /// <summary>
            /// has a .EMF file and a single subdirectory that has an acqu file and fid file
            /// </summary>
            BrukerMALDISpot = 10,

            /// <summary>
            /// Series of zipped subdirectories, with names like 0_R00X329.zip; subdirectories inside the .Zip files have fid files
            /// </summary>
            BrukerMALDIImaging = 11,

            // ReSharper disable once GrammarMistakeInComment

            /// <summary>
            /// Used by Maxis_01; Inside the .D directory is the analysis.baf file; there is also .m subdirectory that has a microTOFQMaxAcquisition.method file; there is not a ser or fid file
            /// </summary>
            BrukerTOFBaf = 12,

            /// <summary>
            /// Used by timsTOFScp01, timsTOFFlex02, and timsTOFFlex02_Imaging; Inside the .D directory are files analysis.tdf and analysis.tdf_bin
            /// </summary>
            BrukerTOFTdf = 13
        }

        /// <summary>
        /// Enum for mzXML and mzML
        /// </summary>
        public enum MSXMLOutputTypeConstants
        {
            /// <summary>
            /// mzXML
            /// </summary>
            mzXML = 0,

            /// <summary>
            /// mzML
            /// </summary>
            mzML = 1,

            /// <summary>
            /// Mascot generic file
            /// </summary>
            mgf = 2
        }

        /// <summary>
        /// Data package file retrieval mode
        /// </summary>
        /// <remarks>Used by Phospho_FDR_Aggregator, Pride_MzXML, and AScore plugins</remarks>
        public enum DataPackageFileRetrievalModeConstants
        {
            /// <summary>
            /// Undefined
            /// </summary>
            Undefined = 0,

            /// <summary>
            /// AScore
            /// </summary>
            Ascore = 1
        }

        /// <summary>
        /// Manager parameters
        /// </summary>
        /// <remarks>Instance of class AnalysisMgrSettings</remarks>
        protected IMgrParams mMgrParams;

        /// <summary>
        /// Working directory
        /// </summary>
        protected string mWorkDir;

        /// <summary>
        /// Job number
        /// </summary>
        protected int mJob;

        /// <summary>
        /// Dataset name
        /// </summary>
        /// <remarks>
        /// mDatasetName is private because dataset name updates
        /// should be done using property DatasetName,
        /// so that the changes will propagate
        /// into DirectorySearch and FileSearch
        /// </remarks>
        private string mDatasetName;

        /// <summary>
        /// Directory space tools
        /// </summary>
        protected readonly DirectorySpaceTools mDirectorySpaceTools;

        /// <summary>
        /// Manager name
        /// </summary>
        protected string mMgrName;

        /// <summary>
        ///Protein database connection string
        /// </summary>
        protected string mFastaToolsCnStr = "";

        /// <summary>
        /// FASTA file name generated by mFastaTools
        /// </summary>
        protected string mFastaFileName = "";

        /// <summary>
        /// FASTA file generation tools
        /// </summary>
        protected OrganismDatabaseHandler.ProteinExport.GetFASTAFromDMS mFastaTools;

        /// <summary>
        /// CDTA utilities
        /// </summary>
        protected readonly CDTAUtilities mCDTAUtilities;

        /// <summary>
        /// FASTA file splitter utility
        /// </summary>
        protected SplitFastaFileUtilities mSplitFastaFileUtility;

        /// <summary>
        /// Last time FASTA splitting progress was reported
        /// </summary>
        protected DateTime mSplitFastaLastUpdateTime;

        /// <summary>
        /// FASTA splitting percent complete
        /// </summary>
        /// <remarks>Value between 0 and 100</remarks>
        protected float mSplitFastaLastPercentComplete;

        private DateTime mLastCDTAUtilitiesUpdateTime;

        private DateTime mFastaToolsLastLogTime;
        private double mFastaToolFractionDoneSaved = -1;

        /// <summary>
        /// MyEMSL utilities
        /// </summary>
        protected MyEMSLUtilities mMyEMSLUtilities;

        private Dictionary<Global.AnalysisResourceOptions, bool> mResourceOptions;
        private bool mAuroraAvailable;

        private bool mMyEMSLSearchDisabled;

        private DataPackageJobInfo mCachedDatasetAndJobInfo;

        private bool mCachedDatasetAndJobInfoIsDefined;

        /// <summary>
        /// Spectrum type classifier
        /// </summary>
        public SpectraTypeClassifier.SpectrumTypeClassifier mSpectraTypeClassifier;

        /// <summary>
        /// File copy utilities
        /// </summary>
        protected FileCopyUtilities mFileCopyUtilities;

        /// <summary>
        /// Dataset name
        /// </summary>
        /// <remarks>Also updates DirectorySearch and FileSearch</remarks>
        public string DatasetName
        {
            get => mDatasetName;
            set
            {
                mDatasetName = value;

                if (DirectorySearchTool != null)
                    DirectorySearchTool.DatasetName = mDatasetName;

                if (FileSearchTool != null)
                    FileSearchTool.DatasetName = mDatasetName;
            }
        }

        /// <summary>
        /// Debug level
        /// </summary>
        /// <remarks>Ranges from 0 (minimum output) to 5 (max detail)</remarks>
        public short DebugLevel => mDebugLevel;

        /// <summary>
        /// Directory search utility
        /// </summary>
        public DirectorySearch DirectorySearchTool { get; private set; }

        /// <summary>
        /// File search utility
        /// </summary>
        public FileSearch FileSearchTool { get; private set; }

        /// <summary>
        /// Manager parameters
        /// </summary>
        public IMgrParams MgrParams => mMgrParams;

        /// <summary>
        /// True when MyEMSL search is disabled
        /// </summary>
        public bool MyEMSLSearchDisabled
        {
            get => mMyEMSLSearchDisabled;
            set
            {
                mMyEMSLSearchDisabled = value;

                if (DirectorySearchTool != null)
                {
                    if (mMyEMSLSearchDisabled && !DirectorySearchTool.MyEMSLSearchDisabled)
                        DirectorySearchTool.MyEMSLSearchDisabled = true;
                }

                if (FileSearchTool != null)
                {
                    if (mMyEMSLSearchDisabled && !FileSearchTool.MyEMSLSearchDisabled)
                        FileSearchTool.MyEMSLSearchDisabled = true;
                }
            }
        }

        /// <summary>
        /// MyEMSL utilities
        /// </summary>
        public MyEMSLUtilities MyEMSLUtils => mMyEMSLUtilities;

        /// <summary>
        /// Explanation of what happened to last operation this class performed
        /// </summary>
        /// <remarks>
        /// If the resourcer decides to skip the step tool, this message will be stored in the Completion_Message field in the database
        /// </remarks>
        public string Message => mMessage;

        /// <summary>
        /// Additional status message
        /// </summary>
        /// <remarks>
        /// If the resourcer decides to skip the step tool, this message will be stored in the Evaluation_Message field in the database
        /// </remarks>
        public string EvalMessage { get; protected set; }

        /// <summary>
        /// The resourcer sets this to true if the job cannot be run due to not enough free memory
        /// </summary>
        public bool InsufficientFreeMemory => mInsufficientFreeMemory;

        /// <summary>
        /// The resourcer sets this to true if we need to abort processing as soon as possible due to a critical error
        /// </summary>
        public bool NeedToAbortProcessing => mNeedToAbortProcessing;

        /// <summary>
        /// True when the step tool contains the text DataExtractor
        /// </summary>
        private bool RunningDataExtraction => StepToolName.IndexOf("DataExtractor", StringComparison.OrdinalIgnoreCase) >= 0;

        /// <summary>
        /// Step tool name
        /// </summary>
        /// <remarks>
        /// Step tools are defined in plugin_info.xml
        /// Example step tool names: msgfplus, msalign, and toppic
        /// </remarks>
        public string StepToolName { get; private set; }

        /// <summary>
        /// Work directory path
        /// </summary>
        public string WorkDir => mWorkDir;

        /// <summary>
        /// Constructor
        /// </summary>
        protected AnalysisResources() : base("AnalysisResources")
        {
            mCDTAUtilities = new CDTAUtilities();
            RegisterEvents(mCDTAUtilities);
            mCDTAUtilities.ProgressUpdate -= ProgressUpdateHandler;
            mCDTAUtilities.ProgressUpdate += CDTAUtilities_ProgressEvent;

            mDirectorySpaceTools = new DirectorySpaceTools(true);
        }

        /// <summary>
        /// Register event handlers
        /// </summary>
        /// <param name="processingClass">Processing class</param>
        /// <param name="writeDebugEventsToLog">If true, write debug events to the log</param>
        protected sealed override void RegisterEvents(IEventNotifier processingClass, bool writeDebugEventsToLog = true)
        {
            base.RegisterEvents(processingClass, writeDebugEventsToLog);
        }

        /// <summary>
        /// Initialize class
        /// </summary>
        /// <param name="stepToolName">Name of the current step tool</param>
        /// <param name="mgrParams">Manager parameter object</param>
        /// <param name="jobParams">Job parameter object</param>
        /// <param name="statusTools">Object for status reporting</param>
        /// <param name="myEMSLUtilities">MyEMSL download Utilities (can be null)</param>
        public virtual void Setup(
            string stepToolName,
            IMgrParams mgrParams,
            IJobParams jobParams,
            IStatusFile statusTools,
            MyEMSLUtilities myEMSLUtilities)
        {
            StepToolName = stepToolName;

            mMgrParams = mgrParams;
            mJobParams = jobParams;
            mStatusTools = statusTools;

            TraceMode = mgrParams.TraceMode;

            mDebugLevel = (short)mMgrParams.GetParam("DebugLevel", 1);
            mFastaToolsCnStr = mMgrParams.GetParam("FastaCnString");
            mMgrName = mMgrParams.ManagerName;

            mWorkDir = mMgrParams.GetParam("WorkDir");

            var jobNum = mJobParams.GetParam(AnalysisJob.STEP_PARAMETERS_SECTION, "Job");

            if (!string.IsNullOrEmpty(jobNum))
            {
                int.TryParse(jobNum, out mJob);
            }

            DatasetName = GetDatasetName(mJobParams, string.Empty);

            InitFileTools(mMgrName, mDebugLevel);

            mMyEMSLUtilities = myEMSLUtilities ?? new MyEMSLUtilities(mDebugLevel, mWorkDir);
            RegisterEvents(mMyEMSLUtilities);

            mFileCopyUtilities = new FileCopyUtilities(mFileTools, mMyEMSLUtilities, mDebugLevel);
            RegisterEvents(mFileCopyUtilities);

            mFileCopyUtilities.ResetTimestampForQueueWaitTime += FileCopyUtilities_ResetTimestampForQueueWaitTime;

            mFileCopyUtilities.CopyWithLocksComplete += FileCopyUtilities_CopyWithLocksComplete;

            mResourceOptions = new Dictionary<Global.AnalysisResourceOptions, bool>();
            SetOption(Global.AnalysisResourceOptions.OrgDbRequired, false);
            SetOption(Global.AnalysisResourceOptions.MyEMSLSearchDisabled, false);

            mAuroraAvailable = mMgrParams.GetParam("AuroraAvailable", true);

            var myEmslAvailable = mMgrParams.GetParam("MyEmslAvailable", true);

            DirectorySearchTool = new DirectorySearch(
                mFileCopyUtilities, mJobParams, mMyEMSLUtilities,
                mDatasetName, mDebugLevel, mAuroraAvailable);

            RegisterEvents(DirectorySearchTool);

            DirectorySearchTool.MyEMSLSearchDisabled = mMyEMSLSearchDisabled || !myEmslAvailable;

            FileSearchTool = new FileSearch(
                mFileCopyUtilities, DirectorySearchTool, mMyEMSLUtilities,
                mMgrParams, mJobParams, mDatasetName, mDebugLevel, mWorkDir, mAuroraAvailable);

            RegisterEvents(FileSearchTool);

            FileSearchTool.MyEMSLSearchDisabled = mMyEMSLSearchDisabled || !myEmslAvailable;
        }

        /// <summary>
        /// Copy the generated FASTA file to the remote host that will be running this job
        /// </summary>
        /// <param name="transferUtility">Remote transfer utility</param>
        /// <returns>True if success, false if an error</returns>
        protected bool CopyGeneratedOrgDBToRemote(RemoteTransferUtility transferUtility)
        {
            var dbFilename = mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, JOB_PARAM_GENERATED_FASTA_NAME);

            if (string.IsNullOrWhiteSpace(dbFilename))
            {
                LogError("Cannot copy the generated FASTA remotely; parameter " + JOB_PARAM_GENERATED_FASTA_NAME + " is empty");
                return false;
            }

            var orgDbDirPath = mMgrParams.GetParam(MGR_PARAM_ORG_DB_DIR);

            if (string.IsNullOrWhiteSpace(orgDbDirPath))
            {
                LogError("Cannot copy the generated FASTA remotely; manager parameter OrgDBDir is empty");
                return false;
            }

            var orgDbDir = new DirectoryInfo(orgDbDirPath);

            var sourceFasta = new FileInfo(Path.Combine(orgDbDir.FullName, dbFilename));

            if (!sourceFasta.Exists)
            {
                LogError("Cannot copy the generated FASTA remotely; file not found: " + sourceFasta.FullName);
                return false;
            }

            // Find .hashcheck files
            var hashcheckFiles = orgDbDir.GetFiles(sourceFasta.Name + "*" + OrganismDatabaseHandler.ProteinExport.GetFASTAFromDMS.HashcheckSuffix);

            if (hashcheckFiles.Length == 0)
            {
                LogError("Local hashcheck file not found for " + sourceFasta.FullName + "; cannot copy remotely");
                return false;
            }

            var sourceHashcheck = (from item in hashcheckFiles orderby item.LastWriteTime descending select item).First();

            LogDebug("Verifying that the generated FASTA file exists on the remote host");

            // Check whether the file needs to be copied
            // Skip the copy if it exists and has the same size
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

                    if (string.Equals(extension, FASTA_FILE_EXTENSION, StringComparison.OrdinalIgnoreCase))
                        remoteFasta = remoteFile.Value;

                    if (string.Equals(extension, OrganismDatabaseHandler.ProteinExport.GetFASTAFromDMS.HashcheckSuffix, StringComparison.OrdinalIgnoreCase))
                        remoteHashcheck = remoteFile.Value;
                }

                var filesMatch = RemoteFastaFilesMatch(sourceFasta, sourceHashcheck, remoteFasta, remoteHashcheck, transferUtility);

                if (filesMatch && remoteFasta != null)
                {
                    LogDebug("Using existing FASTA file {0} on {1}", remoteFasta.FullName, transferUtility.RemoteHostName);
                    return true;
                }
            }
            else
            {
                LogDebug("FASTA file not found on remote host; copying {0} to {1}", sourceFasta.Name, transferUtility.RemoteHostName);
            }

            // Find the files to copy (skipping the .localhashcheck file)
            var sourceFiles = new List<FileInfo>();

            foreach (var sourceFile in orgDbDir.GetFiles(fileMatchSpec))
            {
                if (string.Equals(sourceFile.Extension, LOCALHASHCHECK_EXTENSION))
                    continue;

                sourceFiles.Add(sourceFile);
            }

            var success = transferUtility.CopyFilesToRemote(sourceFiles, transferUtility.RemoteOrgDBPath, useLockFile: true);

            if (success)
                return true;

            LogError("Error copying {0} to {1} on {2}", sourceFasta.Name, transferUtility.RemoteOrgDBPath, transferUtility.RemoteHostName);
            return false;
        }

        // Ignore Spelling: work dir

        /// <summary>
        /// Copy files in the working directory to a remote host, skipping files in filesToIgnore
        /// </summary>
        /// <remarks>This method is called by step tools that override CopyResourcesToRemote</remarks>
        /// <param name="transferUtility">file transfer utility</param>
        /// <param name="filesToIgnore">Names of files to ignore</param>
        /// <returns>True if success, otherwise false</returns>
        protected bool CopyWorkDirFilesToRemote(RemoteTransferUtility transferUtility, SortedSet<string> filesToIgnore)
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

                // Create the destination directory
                var remoteDirectoryPath = transferUtility.RemoteJobStepWorkDirPath;
                var remoteHost = transferUtility.RemoteHostName;

                var targetDirectoryVerified = transferUtility.CreateRemoteDirectory(remoteDirectoryPath);

                if (!targetDirectoryVerified)
                {
                    LogError("Unable to create working directory {0} on host {1}", remoteDirectoryPath, remoteHost);
                    UpdateStatusMessage("Unable to create working directory on remote host " + remoteHost);
                    return false;
                }

                // Make sure the target working directory is empty
                transferUtility.DeleteRemoteWorkDir(true);

                // Copy the files
                var success = transferUtility.CopyFilesToRemote(filesToCopy, remoteDirectoryPath);

                if (success)
                {
                    LogMessage("Copied {0} files to {1} on host {2}", filesToCopy.Count, remoteDirectoryPath, remoteHost);
                    return true;
                }

                LogError("Failure copying {0} files to {1} on host {2}", filesToCopy.Count, remoteDirectoryPath, remoteHost);
                UpdateStatusMessage("Failure copying required files to remote host " + remoteHost);
                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception copying resources to remote host " + transferUtility.RemoteHostName, ex);
                return false;
            }
        }

        /// <summary>
        /// Call this method to copy files from the working directory to a remote host for remote processing
        /// Plugins that implement this will skip files that are not be needed by the ToolRunner class of the plugin
        /// Plugins should also copy FASTA files if appropriate
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        /// <exception cref="NotImplementedException"></exception>
        public virtual bool CopyResourcesToRemote(RemoteTransferUtility transferUtility)
        {
            throw new NotImplementedException("Plugin " + StepToolName + " must implement CopyResourcesToRemote to allow for remote processing");
        }

        /// <summary>
        /// Abstract method to retrieve resources
        /// </summary>
        /// <remarks>Each step tool implements this method</remarks>
        public abstract CloseOutType GetResources();

        /// <summary>
        /// Retrieve a true/false option of type AnalysisResourceOptions
        /// </summary>
        /// <param name="resourceOption">Analysis resource option enum</param>
        public bool GetOption(Global.AnalysisResourceOptions resourceOption)
        {
            if (mResourceOptions == null)
                return false;

            if (mResourceOptions.TryGetValue(resourceOption, out var enabled))
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
            var myEmslAvailable = mMgrParams.GetParam("MyEmslAvailable", true);

            if (!mMyEMSLSearchDisabled && myEmslAvailable)
            {
                if (!MyEMSLUtils.CertificateFileExists(out var errorMessage))
                {
                    LogError(errorMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            var success = GetExistingJobParametersFile();

            if (success)
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
        }

        /// <summary>
        /// Set a true/false option of type AnalysisResourceOptions
        /// </summary>
        /// <param name="resourceOption">Analysis resource option enum</param>
        /// <param name="enabled">Value to assign</param>
        public void SetOption(Global.AnalysisResourceOptions resourceOption, bool enabled)
        {
            mResourceOptions ??= new Dictionary<Global.AnalysisResourceOptions, bool>();

            // Add/update resourceOption
            mResourceOptions[resourceOption] = enabled;

            if (resourceOption == Global.AnalysisResourceOptions.MyEMSLSearchDisabled)
            {
                MyEMSLSearchDisabled = enabled;
            }
        }

        /// <summary>
        /// Add a filename extension to not move to the results directory
        /// </summary>
        /// <remarks>Can be a file extension (like .raw) or even a partial file name like _peaks.txt</remarks>
        /// <param name="extension">File extension or file name suffix</param>
        public void AddResultFileExtensionToSkip(string extension)
        {
            if (string.IsNullOrWhiteSpace(extension))
                return;

            mJobParams.AddResultFileExtensionToSkip(extension);
        }

        /// <summary>
        /// Add a filename to not move to the results directory
        /// </summary>
        /// <remarks>FileName can be a file path; only the filename will be stored in mResultFilesToSkip</remarks>
        /// <param name="sourceFilename">File name or path</param>
        public void AddResultFileToSkip(string sourceFilename)
        {
            if (string.IsNullOrWhiteSpace(sourceFilename))
                return;

            mJobParams.AddResultFileToSkip(sourceFilename);
        }

        /// <summary>
        /// Appends file specified file path to the JobInfo file for the given Job
        /// </summary>
        /// <param name="job">Job number</param>
        /// <param name="filePath">File path</param>
        protected void AppendToJobInfoFile(int job, string filePath)
        {
            var jobInfoFilePath = DataPackageFileHandler.GetJobInfoFilePath(job, mWorkDir);

            using var writer = new StreamWriter(new FileStream(jobInfoFilePath, FileMode.Append, FileAccess.Write, FileShare.Read));

            writer.WriteLine(filePath);
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
        /// <remarks>
        /// Typical steps for using lock files to assure that only one manager is creating a specific file
        /// 1. Call CheckForLockFile() to check for a lock file; wait for it to age
        /// 2. Once CheckForLockFile() exits, check for the required data file; exit the method if the desired file is found
        /// 3. If the file was not found, create a new lock file by calling CreateLockFile()
        /// 4. Do the work and create the data file, including copying to the central location
        /// 5. Delete the lock file by calling DeleteLockFile() or by deleting the file path returned by CreateLockFile()
        /// </remarks>
        /// <param name="dataFilePath">Data file path</param>
        /// <param name="dataFileDescription">User-friendly description of the data file, e.g. LipidMapsDB</param>
        /// <param name="statusTools">Status Tools object</param>
        /// <param name="maxWaitTimeMinutes">Maximum age of the lock file</param>
        /// <param name="logIntervalMinutes">Log interval, in minutes</param>
        public static void CheckForLockFile(
            string dataFilePath,
            string dataFileDescription,
            IStatusFile statusTools,
            int maxWaitTimeMinutes = 120,
            int logIntervalMinutes = 5)
        {
            if (dataFilePath.EndsWith(Global.LOCK_FILE_EXTENSION, StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException("dataFilePath may not end in .lock", nameof(dataFilePath));

            var waitingForLockFile = false;
            var lockFileCreated = DateTime.UtcNow;

            // Look for a recent .lock file
            var lockFile = new FileInfo(dataFilePath + Global.LOCK_FILE_EXTENSION);

            if (lockFile.Exists)
            {
                if (DateTime.UtcNow.Subtract(lockFile.LastWriteTimeUtc).TotalMinutes < maxWaitTimeMinutes)
                {
                    waitingForLockFile = true;
                    lockFileCreated = lockFile.LastWriteTimeUtc;

                    var debugMessage = dataFileDescription +
                        " lock file found; will wait for file to be deleted or age; " +
                        lockFile.Name + " created " + lockFile.LastWriteTime.ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT);
                    LogTools.LogDebug(debugMessage);
                }
                else
                {
                    // Lock file has aged; delete it
                    lockFile.Delete();
                }
            }

            if (waitingForLockFile)
            {
                var lastProgressTime = DateTime.UtcNow;

                if (logIntervalMinutes < 1)
                    logIntervalMinutes = 1;

                while (waitingForLockFile)
                {
                    // Wait 5 seconds
                    Global.IdleLoop(5);

                    lockFile.Refresh();

                    if (!lockFile.Exists)
                    {
                        // Lock file no longer exists
                        waitingForLockFile = false;
                    }
                    else if (DateTime.UtcNow.Subtract(lockFileCreated).TotalMinutes > maxWaitTimeMinutes)
                    {
                        // We have waited too long
                        waitingForLockFile = false;
                    }
                    else
                    {
                        if (DateTime.UtcNow.Subtract(lastProgressTime).TotalMinutes >= logIntervalMinutes)
                        {
                            LogDebugMessage("Waiting for lock file " + lockFile.Name, statusTools);
                            lastProgressTime = DateTime.UtcNow;
                        }
                    }
                }

                // Check for the lock file one more time
                lockFile.Refresh();

                if (lockFile.Exists)
                {
                    // Lock file is over 2 hours old; delete it
                    Global.DeleteLockFile(dataFilePath);
                }
            }
        }

        // Ignore Spelling: yyyy-MM-dd, hh:mm:ss tt, nn

        /// <summary>
        /// Create a new lock file named dataFilePath + ".lock"
        /// </summary>
        /// <remarks>An exception will be thrown if the lock file already exists</remarks>
        /// <param name="dataFilePath">Data file path</param>
        /// <param name="taskDescription">Description of current task; will be written the lock file, followed by " at yyyy-MM-dd hh:mm:ss tt"</param>
        /// <returns>Full path to the lock file</returns>
        public static string CreateLockFile(string dataFilePath, string taskDescription)
        {
            var lockFilePath = dataFilePath + Global.LOCK_FILE_EXTENSION;

            using var writer = new StreamWriter(new FileStream(lockFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.Read));

            writer.WriteLine(taskDescription + " at " + DateTime.Now.ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT));

            return lockFilePath;
        }

        /// <summary>
        /// Copies specified file from storage server to local working directory
        /// </summary>
        /// <remarks>If the file was found in MyEMSL, sourceDirectoryPath will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
        /// <param name="sourceFileName">Name of file to copy</param>
        /// <param name="sourceDirectoryPath">Path to directory where input file is located</param>
        /// <param name="targetDirectoryPath">Destination directory for file copy</param>
        /// <returns>True if success, false if an error</returns>
        protected bool CopyFileToWorkDir(string sourceFileName, string sourceDirectoryPath, string targetDirectoryPath)
        {
            return mFileCopyUtilities.CopyFileToWorkDir(sourceFileName, sourceDirectoryPath, targetDirectoryPath);
        }

        /// <summary>
        /// Copies specified file from storage server to local working directory
        /// </summary>
        /// <remarks>If the file was found in MyEMSL, sourceDirectoryPath will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
        /// <param name="sourceFileName">Name of file to copy</param>
        /// <param name="sourceDirectoryPath">Path to directory where input file is located</param>
        /// <param name="targetDirectoryPath">Destination directory for file copy</param>
        /// <param name="logMsgTypeIfNotFound">Type of message to log if the file is not found</param>
        /// <returns>True if success, false if an error</returns>
        public bool CopyFileToWorkDir(string sourceFileName, string sourceDirectoryPath, string targetDirectoryPath, BaseLogger.LogLevels logMsgTypeIfNotFound)
        {
            return mFileCopyUtilities.CopyFileToWorkDir(sourceFileName, sourceDirectoryPath, targetDirectoryPath, logMsgTypeIfNotFound);
        }

        /// <summary>
        /// Creates a FASTA file using Protein_Exporter.dll
        /// </summary>
        /// <param name="proteinCollectionInfo">Protein collection info instance</param>
        /// <param name="targetDirectory">Directory where file will be created</param>
        /// <param name="decoyProteinsUseXXX">When true, decoy protein names start with XXX_</param>
        /// <param name="previewMode">Set to true to show the filename that would be retrieved</param>
        /// <returns>True if success, false if an error</returns>
        protected bool CreateFastaFile(
            ProteinCollectionInfo proteinCollectionInfo,
            string targetDirectory,
            bool decoyProteinsUseXXX,
            bool previewMode)
        {
            if (mDebugLevel >= 1)
            {
                // Creating FASTA file in ...
                // or
                // Preview retrieval of FASTA file ...
                LogMessage("{0} FASTA file in {1}",
                    previewMode ? "Preview retrieval of" : "Creating",
                    targetDirectory);
            }

            if (!Directory.Exists(targetDirectory))
            {
                Directory.CreateDirectory(targetDirectory);
            }

            // Instantiate FASTA tool if not already done
            if (mFastaTools == null && string.IsNullOrWhiteSpace(mFastaToolsCnStr))
            {
                UpdateStatusMessage("Protein database connection string not specified");
                LogMessage("Error in CreateFastaFile: " + mMessage, 0, true);
                return false;
            }

            var retryCount = 1;

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(mFastaToolsCnStr, mMgrName);

            while (true)
            {
                try
                {
                    mFastaTools = new OrganismDatabaseHandler.ProteinExport.GetFASTAFromDMS(connectionStringToUse)
                    {
                        DecoyProteinsUseXXX = decoyProteinsUseXXX
                    };

                    RegisterEvents(mFastaTools);

                    mFastaTools.FileGenerationStarted += FastaTools_FileGenerationStarted;
                    mFastaTools.FileGenerationProgress += FastaTools_FileGenerationProgress;
                    mFastaTools.FileGenerationCompleted += FastaTools_FileGenerationCompleted;

                    break;
                }
                catch (Exception ex)
                {
                    // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                    if (retryCount > 1)
                    {
                        LogError("Error instantiating GetFASTAFromDMS", ex);
                        // Sleep 20 seconds after the first failure and 30 seconds after the second failure
                        if (retryCount == 3)
                        {
                            Global.IdleLoop(20);
                        }
                        else
                        {
                            Global.IdleLoop(30);
                        }
                    }
                    else
                    {
                        UpdateStatusMessage("Error retrieving protein collection or legacy FASTA file: ");

                        if (ex.Message.Contains("could not open database connection"))
                        {
                            mMessage += "could not open database connection";
                        }
                        else
                        {
                            mMessage += ex.Message;
                        }

                        LogError(mMessage, ex);
                        LogDebugMessage("Connection string: " + connectionStringToUse);
                        LogDebugMessage("Current user: " + Environment.UserName);

                        if (ex.Message.IndexOf("The timeout period elapsed prior to obtaining a connection from the pool", StringComparison.OrdinalIgnoreCase) >= 0)
                        {
                            // This error can happen if too many analysis jobs run in a row and GetFASTAFromDMS doesn't free connections to the database

                            // Could not open database connection after 6 tries, Timeout expired.
                            // The timeout period elapsed prior to obtaining a connection from the pool.
                            // This may have occurred because all pooled connections were in use and max pool size was reached.
                            mNeedToAbortProcessing = true;
                        }
                        return false;
                    }
                    retryCount--;
                }
            }

            // Initialize FASTA generation state variables
            mFastaFileName = string.Empty;

            // Set up variables for FASTA creation call

            if (!proteinCollectionInfo.IsValid)
            {
                if (string.IsNullOrWhiteSpace(proteinCollectionInfo.ErrorMessage))
                {
                    UpdateStatusMessage("Unknown error determining the FASTA file or protein collection to use; unable to obtain FASTA file");
                }
                else
                {
                    UpdateStatusMessage(proteinCollectionInfo.ErrorMessage + "; unable to obtain FASTA file");
                }

                LogMessage("Error in CreateFastaFile: " + mMessage, 0, true);
                return false;
            }

            string legacyFastaToUse;
            var orgDBDescription = proteinCollectionInfo.OrgDBDescription;

            // The ToolName job parameter holds the name of the pipeline script we are executing
            var scriptName = mJobParams.GetParam("ToolName");

            if (proteinCollectionInfo.UsingSplitFasta && (scriptName.StartsWith("FragPipe", StringComparison.OrdinalIgnoreCase) || scriptName.StartsWith("MSFragger", StringComparison.OrdinalIgnoreCase)))
            {
                LogError(string.Format(
                    "The settings file for this {0} job has \"SplitFasta\" defined; " +
                    "it should instead use \"DatabaseSplitCount\" to enable FASTA file splitting (see, for example, FragPipe_SplitFASTA_10x.xml)",
                    scriptName));

                return false;
            }

            if (proteinCollectionInfo.UsingSplitFasta && !RunningDataExtraction)
            {
                if (!proteinCollectionInfo.UsingLegacyFasta)
                {
                    LogError("Cannot use protein collections when running a SplitFasta job; choose a Legacy FASTA file instead");
                    return false;
                }

                // Running a SplitFasta job; need to update the name of the FASTA file to be of the form FastaFileName_NNx_nn.fasta
                // where NN is the number of total cloned steps and nn is this job's specific step number

                legacyFastaToUse = GetSplitFastaFileName(mJobParams, out var errorMessage, out var numberOfClonedSteps);

                if (!string.IsNullOrWhiteSpace(errorMessage))
                {
                    UpdateStatusMessage(errorMessage);
                }

                if (string.IsNullOrEmpty(legacyFastaToUse))
                {
                    // The error should have already been logged
                    return false;
                }

                orgDBDescription = "Legacy DB: " + legacyFastaToUse;

                // Lookup the MSGFPlus Index Folder path
                var msgfPlusIndexFilesDirPathLegacyDB = mMgrParams.GetParam("MSGFPlusIndexFilesFolderPathLegacyDB", @"\\Proto-7\MSGFPlus_Index_Files");

                if (string.IsNullOrWhiteSpace(msgfPlusIndexFilesDirPathLegacyDB))
                {
                    msgfPlusIndexFilesDirPathLegacyDB = @"\\Proto-7\MSGFPlus_Index_Files\Other";
                }
                else
                {
                    msgfPlusIndexFilesDirPathLegacyDB = Path.Combine(msgfPlusIndexFilesDirPathLegacyDB, "Other");
                }

                if (mDebugLevel >= 1)
                {
                    LogMessage("Verifying that split FASTA file exists: " + legacyFastaToUse);
                }

                // Make sure the original FASTA file has already been split into the appropriate number parts and that DMS knows about them
                mSplitFastaFileUtility = new SplitFastaFileUtilities(numberOfClonedSteps, mFileCopyUtilities, mMgrParams, mJobParams);
                RegisterEvents(mSplitFastaFileUtility);

                if (!mSplitFastaFileUtility.DefineConnectionStrings())
                {
                    return false;
                }

                // Use a custom handler for ProgressUpdate
                mSplitFastaFileUtility.ProgressUpdate -= ProgressUpdateHandler;
                mSplitFastaFileUtility.ProgressUpdate += SplitFastaFileUtility_ProgressUpdate;

                mSplitFastaFileUtility.SplittingBaseFastaFile += SplitFastaFileUtility_SplittingBaseFastaFile;

                mSplitFastaFileUtility.MSGFPlusIndexFilesFolderPathLegacyDB = msgfPlusIndexFilesDirPathLegacyDB;

                mSplitFastaLastUpdateTime = DateTime.UtcNow;
                mSplitFastaLastPercentComplete = 0;

                var success = mSplitFastaFileUtility.ValidateSplitFastaFile(proteinCollectionInfo.LegacyFastaName, legacyFastaToUse);

                if (!success)
                {
                    UpdateStatusMessage(mSplitFastaFileUtility.ErrorMessage);
                    return false;
                }
            }
            else
            {
                legacyFastaToUse = proteinCollectionInfo.LegacyFastaName;
            }

            if (mDebugLevel >= 2)
            {
                var proteinSeqsDBConnectionString = mMgrParams.GetParam("FastaCnString");
                var hostName = DbToolsFactory.GetHostNameFromConnectionString(proteinSeqsDBConnectionString);

                LogMessage(
                    "ProteinCollectionList=" + proteinCollectionInfo.ProteinCollectionList + "; " +
                    "CreationOpts=" + proteinCollectionInfo.ProteinCollectionOptions + "; " +
                    "LegacyFasta=" + legacyFastaToUse + "; " +
                    "ProteinSeqs_DB_Host=" + hostName);
            }

            try
            {
                if (mFastaTools == null)
                {
                    LogError("Call to CreateFastaFile without initializing mFastaTools");
                    return false;
                }

                if (previewMode)
                {
                    if (mDebugLevel >= 1)
                    {
                        LogMessage(
                            "Simulate call to mFastaTools.ExportFASTAFile for " +
                            "ProteinCollectionList '{0}', ProteinCollectionOptions '{1}',  and legacy FASTA {2}",
                            proteinCollectionInfo.ProteinCollectionList,
                            proteinCollectionInfo.ProteinCollectionOptions,
                            legacyFastaToUse);
                    }

                    if ((string.IsNullOrEmpty(proteinCollectionInfo.ProteinCollectionList) ||
                         proteinCollectionInfo.ProteinCollectionList.Equals("na", StringComparison.OrdinalIgnoreCase)) &&
                        !string.IsNullOrEmpty(legacyFastaToUse) && !legacyFastaToUse.Equals("na", StringComparison.OrdinalIgnoreCase))
                    {
                        mFastaFileName = legacyFastaToUse;
                    }
                    else
                    {
                        mFastaFileName = proteinCollectionInfo.ProteinCollectionList;
                    }
                }
                else
                {
                    var hashString = mFastaTools.ExportFASTAFile(
                        proteinCollectionInfo.ProteinCollectionList,
                        proteinCollectionInfo.ProteinCollectionOptions,
                        legacyFastaToUse,
                        targetDirectory);

                    if (string.IsNullOrEmpty(hashString))
                    {
                        // FASTA generator returned empty hash string
                        LogError("mFastaTools.ExportFASTAFile returned an empty Hash string for the OrgDB; unable to continue; " + orgDBDescription);
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                if (ex.Message.StartsWith("Legacy FASTA file not found:", StringComparison.OrdinalIgnoreCase))
                {
                    var rePathMatcher = new Regex(@"not found: (?<SourceDirectory>.+)\\");
                    var reMatch = rePathMatcher.Match(ex.Message);

                    if (reMatch.Success)
                    {
                        UpdateStatusMessage("Legacy FASTA file not found at " + reMatch.Groups["SourceDirectory"].Value);
                    }
                    else
                    {
                        UpdateStatusMessage("Legacy FASTA file not found in the organism directory for this job");
                    }
                }
                else
                {
                    UpdateStatusMessage("Exception generating OrgDb file");
                }

                LogError("Exception generating OrgDb file; " + orgDBDescription, ex);
                return false;
            }

            if (string.IsNullOrEmpty(mFastaFileName))
            {
                // FASTA generator never raised event FileGenerationCompleted
                LogError("mFastaTools did not raise event FileGenerationCompleted; unable to continue; " + orgDBDescription);
                return false;
            }

            var fastaFile = new FileInfo(Path.Combine(targetDirectory, mFastaFileName));

            if (mDebugLevel >= 1)
            {
                // Log the name of the FASTA file we're using
                LogDebugMessage("FASTA generation complete, using database: " + mFastaFileName, null);

                if (mDebugLevel >= 2 && !previewMode)
                {
                    // Also log the file creation and modification dates

                    try
                    {
                        var fastaFileMsg = new StringBuilder();

                        fastaFileMsg.Append("FASTA file last modified: " +
                            GetHumanReadableTimeInterval(
                                DateTime.UtcNow.Subtract(fastaFile.LastWriteTimeUtc)) + " ago at " +
                                            fastaFile.LastWriteTime.ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT));

                        fastaFileMsg.Append("; file created: " +
                            GetHumanReadableTimeInterval(
                                DateTime.UtcNow.Subtract(fastaFile.CreationTimeUtc)) + " ago at " +
                                            fastaFile.CreationTime.ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT));

                        fastaFileMsg.Append("; file size: " + fastaFile.Length + " bytes");

                        LogDebugMessage(fastaFileMsg.ToString());
                    }
                    catch (Exception)
                    {
                        // Ignore errors here
                    }
                }
            }

            if (!previewMode)
            {
                UpdateLastUsedFile(fastaFile);
            }

            // If we got to here, everything worked OK
            return true;
        }

        /// <summary>
        /// Given two dates, returns the most recent date
        /// </summary>
        /// <param name="date1">First date</param>
        /// <param name="date2">Second date</param>
        protected static DateTime DateMax(DateTime date1, DateTime date2)
        {
            if (date1 > date2)
            {
                return date1;
            }

            return date2;
        }

        /// <summary>
        /// Method to disable MyEMSL search
        /// </summary>
        public void DisableMyEMSLSearch()
        {
            mMyEMSLUtilities.ClearDownloadQueue();
            MyEMSLSearchDisabled = true;
        }

        /// <summary>
        /// Determines the most appropriate directory to use to obtain dataset files from
        /// Optionally, can require that a certain file also be present in the directory for it to be deemed valid
        /// If no directory is deemed valid, returns the dataset directory path
        /// </summary>
        /// <remarks>The path returned will be "\\MyEMSL" if the best directory is in MyEMSL</remarks>
        /// <param name="datasetName">Name of the dataset</param>
        /// <param name="fileNameToFind">Optional: Name of a file that must exist in the dataset directory; can contain a wildcard, e.g. *.zip</param>
        /// <param name="directoryNameToFind">Optional: Name of a subdirectory that must exist in the dataset directory; can contain a wildcard, e.g. SEQ*</param>
        /// <param name="maxAttempts">Maximum number of attempts</param>
        /// <param name="logDirectoryNotFound">If true, log a warning if the directory is not found</param>
        /// <param name="retrievingInstrumentDataDir">Set to true when retrieving an instrument data directory</param>
        /// <param name="validDirectoryFound">Output: true if a valid directory is ultimately found, otherwise false</param>
        /// <param name="assumeUnpurged">When true, this method returns the path to the dataset directory on the storage server</param>
        /// <param name="directoryNotFoundMessage">Output: directory not found message</param>
        /// <returns>Path to the most appropriate dataset directory</returns>
        public string FindValidDirectory(
            string datasetName, string fileNameToFind, string directoryNameToFind,
            int maxAttempts, bool logDirectoryNotFound,
            bool retrievingInstrumentDataDir, out bool validDirectoryFound,
            bool assumeUnpurged, out string directoryNotFoundMessage)
        {
            var directoryPath = DirectorySearchTool.FindValidDirectory(
                datasetName, fileNameToFind, directoryNameToFind, maxAttempts, logDirectoryNotFound,
                retrievingInstrumentDataDir,
                assumeUnpurged,
                out validDirectoryFound,
                out directoryNotFoundMessage,
                out _);

            if (!validDirectoryFound && !assumeUnpurged)
            {
                UpdateStatusMessage(directoryNotFoundMessage);
            }

            return directoryPath;
        }

        /// <summary>
        /// Create files _ScanStats.txt and _ScanStatsEx.txt for the given dataset
        /// </summary>
        /// <remarks>
        /// <para>
        /// Only valid for Thermo .Raw files, .UIMF files, and Agilent .d directories
        /// </para>
        /// <para>
        /// Will delete the .Raw (or .UIMF or .d) after creating the ScanStats file
        /// </para>
        /// </remarks>
        /// <returns>True if success, false if a problem</returns>
        protected bool GenerateScanStatsFiles()
        {
            const bool DELETE_LOCAL_FILE_OR_DIRECTORY = true;
            return GenerateScanStatsFiles(DELETE_LOCAL_FILE_OR_DIRECTORY);
        }

        /// <summary>
        /// Create files _ScanStats.txt and _ScanStatsEx.txt for the given dataset
        /// </summary>
        /// <remarks>
        /// Only valid for Thermo .Raw files, .UIMF files, and Agilent .d directories
        /// </remarks>
        /// <param name="deleteLocalDatasetFileOrDirectory">True to delete the .raw file, .UIMF file, or .d directory after creating the ScanStats file</param>
        /// <returns>True if success, false if a problem</returns>
        protected bool GenerateScanStatsFiles(bool deleteLocalDatasetFileOrDirectory)
        {
            var rawDataTypeName = mJobParams.GetParam("RawDataType");
            var datasetID = mJobParams.GetJobParameter("DatasetID", 0);

            var dllFound = GetMsFileInfoScannerDllPath(mMgrParams, out var msFileInfoScannerDLLPath, out var errorMessage);

            if (!dllFound || !string.IsNullOrWhiteSpace(errorMessage))
            {
                LogError(errorMessage);
                return false;
            }

            string inputFileOrDirectoryPath;
            bool directoryBasedDataset;

            // Confirm that this dataset is a Thermo .Raw file, .UIMF file or Agilent .d directory
            switch (GetRawDataType(rawDataTypeName))
            {
                case RawDataTypeConstants.ThermoRawFile:
                    inputFileOrDirectoryPath = mDatasetName + DOT_RAW_EXTENSION;
                    directoryBasedDataset = false;
                    break;

                case RawDataTypeConstants.UIMF:
                    inputFileOrDirectoryPath = mDatasetName + DOT_UIMF_EXTENSION;
                    directoryBasedDataset = false;
                    break;

                case RawDataTypeConstants.AgilentDFolder:
                    inputFileOrDirectoryPath = mDatasetName + DOT_D_EXTENSION;
                    directoryBasedDataset = true;
                    break;

                default:
                    LogError("Invalid dataset type for auto-generating files _ScanStats.txt and _ScanStatsEx.txt: " + rawDataTypeName);
                    return false;
            }

            inputFileOrDirectoryPath = Path.Combine(mWorkDir, inputFileOrDirectoryPath);

            if (!directoryBasedDataset && !File.Exists(inputFileOrDirectoryPath) ||
                directoryBasedDataset && !Directory.Exists(inputFileOrDirectoryPath))
            {
                if (!FileSearchTool.RetrieveSpectra(rawDataTypeName))
                {
                    var extraMsg = mMessage;
                    UpdateStatusMessage(string.Format("Error retrieving spectra {0}", directoryBasedDataset ? "directory" : "file"));

                    if (!string.IsNullOrWhiteSpace(extraMsg))
                    {
                        mMessage += "; " + extraMsg;
                    }

                    LogMessage(mMessage, 0, true);
                    return false;
                }

                // ReSharper disable once RedundantNameQualifier
                if (!mMyEMSLUtilities.ProcessMyEMSLDownloadQueue(mWorkDir, Downloader.DownloadLayout.FlatNoSubdirectories))
                {
                    return false;
                }
            }

            if (!directoryBasedDataset)
            {
                // Make sure the raw data file does not get copied to the results directory
                mJobParams.AddResultFileToSkip(Path.GetFileName(inputFileOrDirectoryPath));
            }

            var scanStatsGenerator = new ScanStatsGenerator(msFileInfoScannerDLLPath, mDebugLevel);
            RegisterEvents(scanStatsGenerator);

            LogMessage("Generating the ScanStats files for " + Path.GetFileName(inputFileOrDirectoryPath));

            // Create files _ScanStats.txt and _ScanStatsEx.txt
            var success = scanStatsGenerator.GenerateScanStatsFiles(inputFileOrDirectoryPath, mWorkDir, datasetID);

            if (success)
            {
                LogMessage("Generated ScanStats file using " + inputFileOrDirectoryPath);

                if (!deleteLocalDatasetFileOrDirectory)
                    return true;

                AppUtils.GarbageCollectNow();

                try
                {
                    if (directoryBasedDataset)
                    {
                        var directoryToDelete = new DirectoryInfo(inputFileOrDirectoryPath);
                        directoryToDelete.Delete(true);
                    }
                    else
                    {
                        File.Delete(inputFileOrDirectoryPath);
                    }
                }
                catch (Exception)
                {
                    // Ignore errors here
                }
            }
            else
            {
                LogError("Error generating ScanStats files with ScanStatsGenerator", scanStatsGenerator.ErrorMessage);

                if (scanStatsGenerator.MSFileInfoScannerErrorCount > 0)
                {
                    LogMessage("MSFileInfoScanner encountered " + scanStatsGenerator.MSFileInfoScannerErrorCount + " errors");
                }
            }

            return success;
        }

        /// <summary>
        /// Return the current dataset and job info
        /// </summary>
        protected DataPackageJobInfo GetCurrentDatasetAndJobInfo()
        {
            const string jobParamsSection = AnalysisJob.JOB_PARAMETERS_SECTION;
            const string peptideSearchSection = AnalysisJob.PEPTIDE_SEARCH_SECTION;

            var jobNumber = mJobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, "Job", 0);
            var dataset = GetDatasetName(mJobParams, mDatasetName);

            var jobInfo = new DataPackageJobInfo(jobNumber, dataset)
            {
                DatasetID = mJobParams.GetJobParameter(jobParamsSection, "DatasetID", 0),
                Instrument = mJobParams.GetJobParameter(jobParamsSection, "Instrument", string.Empty),
                InstrumentGroup = mJobParams.GetJobParameter(jobParamsSection, "InstrumentGroup", string.Empty),
                Experiment = mJobParams.GetJobParameter(jobParamsSection, "Experiment", string.Empty),
                Experiment_Reason = string.Empty,
                Experiment_Comment = string.Empty,
                Experiment_Organism = string.Empty,
                Experiment_NEWT_ID = 0,
                Experiment_NEWT_Name = string.Empty,
                Tool = mJobParams.GetJobParameter(jobParamsSection, "ToolName", string.Empty),
                NumberOfClonedSteps = mJobParams.GetJobParameter("NumberOfClonedSteps", 0),
                ResultType = mJobParams.GetJobParameter(jobParamsSection, "ResultType", string.Empty),
                SettingsFileName = mJobParams.GetJobParameter(jobParamsSection, "SettingsFileName", string.Empty),
                ParameterFileName = mJobParams.GetJobParameter(peptideSearchSection, JOB_PARAM_PARAMETER_FILE, string.Empty),
                GeneratedFASTAFileName = mJobParams.GetJobParameter(jobParamsSection, JOB_PARAM_GENERATED_FASTA_NAME, string.Empty),
                LegacyFastaFileName = mJobParams.GetJobParameter(peptideSearchSection, "LegacyFastaFileName", string.Empty),
                ProteinCollectionList = mJobParams.GetJobParameter(peptideSearchSection, "ProteinCollectionList", string.Empty),
                ProteinOptions = mJobParams.GetJobParameter(peptideSearchSection, "ProteinOptions", string.Empty),
                ServerStoragePath = mJobParams.GetJobParameter(jobParamsSection, "DatasetStoragePath", string.Empty),
                ArchiveStoragePath = mJobParams.GetJobParameter(jobParamsSection, "DatasetArchivePath", string.Empty),
                ResultsFolderName = mJobParams.GetJobParameter(jobParamsSection, JOB_PARAM_INPUT_FOLDER_NAME, string.Empty),
                DatasetFolderName = mJobParams.GetJobParameter(jobParamsSection, JOB_PARAM_DATASET_FOLDER_NAME, string.Empty)
            };

            var sharedResultsDirectories = mJobParams.GetJobParameter(jobParamsSection, JOB_PARAM_SHARED_RESULTS_FOLDERS, string.Empty);

            foreach (var sharedResultsDirectory in sharedResultsDirectories.Split(','))
            {
                if (string.IsNullOrWhiteSpace(sharedResultsDirectory) || jobInfo.SharedResultsFolders.Contains(sharedResultsDirectory))
                    continue;
                jobInfo.SharedResultsFolders.Add(sharedResultsDirectory);
            }

            jobInfo.RawDataType = mJobParams.GetJobParameter(jobParamsSection, "RawDataType", string.Empty);

            return jobInfo;
        }

        /// <summary>
        /// Looks up the storage path for a given data package
        /// </summary>
        /// <param name="connectionString">Database connection string (DMS_Pipeline DB, aka the broker DB)</param>
        /// <param name="dataPackageID">Data Package ID</param>
        /// <returns>Storage path if successful, empty path if an error or unknown data package</returns>
        protected string GetDataPackageStoragePath(string connectionString, int dataPackageID)
        {
            var sqlStr = new StringBuilder();

            sqlStr.Append("SELECT share_path AS storage_path ");
            sqlStr.Append("FROM V_DMS_Data_Packages ");
            sqlStr.Append("WHERE id = " + dataPackageID);

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, mMgrName);

            var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: false);
            var success = dbTools.GetQueryResultsDataTable(sqlStr.ToString(), out var resultSet);

            if (!success)
            {
                const string errorMessage = "GetDataPackageStoragePath; Excessive failures attempting to retrieve data package info from database";
                LogTools.LogError(errorMessage);
                return string.Empty;
            }

            // Verify at least one row returned
            if (resultSet.Rows.Count < 1)
            {
                // No data was returned
                // Log an error

                var errorMessage = "GetDataPackageStoragePath; Data package not found: " + dataPackageID;
                LogTools.LogError(errorMessage);
                return string.Empty;
            }

            var curRow = resultSet.Rows[0];

            return curRow[0].CastDBVal<string>();
        }

        /// <summary>
        /// Determine the dataset name by looking for job parameter DatasetName
        /// </summary>
        /// <param name="jobParams">Job parameters</param>
        /// <param name="valueIfMissing">Dataset name to use if the parameter is not found</param>
        /// <returns>Dataset name, or valueIfMissing if the job parameter is not found</returns>
        private string GetDatasetName(IJobParams jobParams, string valueIfMissing)
        {
            var datasetName = GetDatasetName(jobParams);

            if (!string.IsNullOrWhiteSpace(datasetName))
                return datasetName;

            if (!string.IsNullOrWhiteSpace(valueIfMissing))
                return valueIfMissing;

            var dataPackageID = jobParams.GetJobParameter(AnalysisJob.JOB_PARAMETERS_SECTION, "DataPackageID", 0);

            if (dataPackageID > 0)
            {
                LogDebug("Could not determine the dataset name since this job is for data package {0} and thus does not have job parameter {1}", dataPackageID, JOB_PARAM_DATASET_NAME);
                return string.Empty;
            }

            LogWarning("Could not determine the dataset name since missing job parameter {0}", JOB_PARAM_DATASET_NAME);

            return string.Empty;
        }

        /// <summary>
        /// Determine the dataset name by looking for job parameters DatasetName or DatasetNum
        /// </summary>
        /// <param name="jobParams">Job parameters</param>
        /// <returns>Dataset name, or an empty string if the job parameter is not found</returns>
        public static string GetDatasetName(IJobParams jobParams)
        {
            var datasetName = jobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, JOB_PARAM_DATASET_NAME);

            if (!string.IsNullOrWhiteSpace(datasetName))
                return datasetName;

#pragma warning disable CS0618
            var legacyDatasetName = jobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, JOB_PARAM_DATASET_NAME_LEGACY);
#pragma warning restore CS0618

            return string.IsNullOrWhiteSpace(legacyDatasetName) ? string.Empty : legacyDatasetName;
        }

        /// <summary>
        /// Examines the directory tree in directoryPath to find a directory with a name like 2013_2
        /// </summary>
        /// <param name="directoryPath">Directory path</param>
        /// <returns>Matching directory name if found, otherwise an empty string</returns>
        public static string GetDatasetYearQuarter(string directoryPath)
        {
            if (string.IsNullOrEmpty(directoryPath))
            {
                return string.Empty;
            }

            // RegEx to find the year_quarter directory name
            // Valid matches include: 2014_1, 2014_01, 2014_4
            var reYearQuarter = new Regex("^[0-9]{4}_0*[1-4]$", RegexOptions.Compiled);

            // Split directoryPath on the path separator
            var directoryPathNames = directoryPath.Split(Path.DirectorySeparatorChar).ToList();
            directoryPathNames.Reverse();

            foreach (var directoryName in directoryPathNames)
            {
                var reMatch = reYearQuarter.Match(directoryName);

                if (reMatch.Success)
                {
                    return reMatch.Value;
                }
            }

            return string.Empty;
        }

        /// <summary>
        /// Examine the FASTA file to determine the fraction of the proteins that are decoy (reverse) proteins
        /// </summary>
        /// <remarks>Decoy proteins start with decoyProteinPrefix</remarks>
        /// <param name="fastaFile">FASTA file to examine</param>
        /// <param name="decoyPrefix">Decoy protein prefix (case-sensitive)</param>
        /// <param name="proteinCount">Output: total protein count</param>
        /// <returns>Fraction of the proteins that are decoy (for example 0.5 if half of the proteins start with Reversed_)</returns>
        public static double GetDecoyFastaCompositionStats(FileInfo fastaFile, string decoyPrefix, out int proteinCount)
        {
            return GetDecoyFastaCompositionStats(fastaFile, new List<string> { decoyPrefix }, out proteinCount);
        }

        /// <summary>
        /// Examine the FASTA file to determine the fraction of the proteins that are decoy (reverse) proteins
        /// </summary>
        /// <remarks>Decoy proteins start with decoyProteinPrefix</remarks>
        /// <param name="fastaFile">FASTA file to examine</param>
        /// <param name="decoyPrefixes">Decoy protein prefixes (case-sensitive)</param>
        /// <param name="proteinCount">Output: total protein count</param>
        /// <returns>Fraction of the proteins that are decoy (for example 0.5 if half of the proteins start with Reversed_)</returns>
        public static double GetDecoyFastaCompositionStats(FileInfo fastaFile, List<string> decoyPrefixes, out int proteinCount)
        {
            // Look for protein names that look like:
            // >decoyProteinPrefix
            // where
            // decoyProteinPrefix is typically XXX. or XXX_ or Reversed_

            var prefixesToFind = new List<string>();

            // ReSharper disable once ForeachCanBeConvertedToQueryUsingAnotherGetEnumerator
            foreach (var decoyPrefix in decoyPrefixes)
            {
                prefixesToFind.Add(string.Format(">{0}", decoyPrefix));
            }

            var forwardProteinCount = 0;
            var reverseProteinCount = 0;

            using var reader = new StreamReader(new FileStream(fastaFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

            while (!reader.EndOfStream)
            {
                var dataLine = reader.ReadLine();

                if (string.IsNullOrWhiteSpace(dataLine))
                {
                    continue;
                }

                if (!dataLine.StartsWith(">"))
                    continue;

                // Protein header line found

                var isDecoy = prefixesToFind.Any(prefixToFind => dataLine.StartsWith(prefixToFind));

                if (isDecoy)
                {
                    reverseProteinCount++;
                }
                else
                {
                    forwardProteinCount++;
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
        public static List<string> GetDefaultDecoyPrefixes()
        {
            // Decoy proteins created by MS-GF+ start with XXX_
            // Decoy proteins created by DMS start with Reversed_ or XXX_
            return new List<string> {
                "Reversed_",
                "XXX_",
                "XXX.",
                "XXX:"
            };
        }

        /// <summary>
        /// Return a SortedSet with the default files from the WorkDir to ignore
        /// Used by CopyResourcesToRemote when calling CopyWorkDirFilesToRemote
        /// </summary>
        protected SortedSet<string> GetDefaultWorkDirFilesToIgnore()
        {
            // Construct the JobParameters filename, for example JobParameters_1394245.xml
            // We skip this file because the remote manager loads parameters from JobParams.xml
            // (JobParams.xml has more info than the JobParameters_1394245.xml file)
            var jobParametersFilename = AnalysisJob.JobParametersFilename(mJob);

            var filesToIgnore = new SortedSet<string> { jobParametersFilename };

            return filesToIgnore;
        }

        /// <summary>
        /// Look for a JobParameters file from the previous job step
        /// If found, copy to the working directory, naming it JobParameters_JobNum_PreviousStep.xml
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        private bool GetExistingJobParametersFile()
        {
            if (Global.OfflineMode)
                return true;

            try
            {
                var stepNum = mJobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, "Step", 1);

                if (stepNum == 1)
                {
                    // This is the first step; nothing to retrieve
                    return true;
                }

                // Check whether this is an aggregation job by looking for job parameter DataPackageID
                //   When 0, we are processing a single dataset, and we thus need to include the dataset name, generating a path like \\proto-4\DMS3_Xfer\QC_Dataset\MXQ202103151122_Auto1880613
                //   When positive, we are processing datasets in a data package, and we thus want a path without the dataset name, generating a path like \\proto-9\MaxQuant_Staging\MXQ202103161252_Auto1880833

                var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);
                var includeDatasetName = dataPackageID <= 0;

                var transferDirectoryPath = GetTransferDirectoryPathForJobStep(useInputDirectory: true, includeDatasetName: includeDatasetName);

                if (string.IsNullOrEmpty(transferDirectoryPath))
                {
                    // Transfer directory parameter is empty; nothing to retrieve
                    return true;
                }

                // Construct the filename, for example JobParameters_1394245.xml
                var jobParametersFilename = AnalysisJob.JobParametersFilename(mJob);
                var sourceFile = new FileInfo(Path.Combine(transferDirectoryPath, jobParametersFilename));

                if (!sourceFile.Exists)
                {
                    // File not found; nothing to copy
                    return true;
                }

                // Copy the file, renaming to avoid a naming collision
                var destinationFilePath = Path.Combine(mWorkDir, Path.GetFileNameWithoutExtension(sourceFile.Name) + "_PreviousStep.xml");

                if (mFileCopyUtilities.CopyFileWithRetry(sourceFile.FullName, destinationFilePath, overwrite: true, maxCopyAttempts: 3))
                {
                    if (mDebugLevel > 3)
                    {
                        LogDebugMessage("GetExistingJobParametersFile, File copied:  " + sourceFile.FullName);
                    }
                }
                else
                {
                    LogError("Error in GetExistingJobParametersFile copying file " + sourceFile.FullName);
                    return false;
                }

                var sourceJobParamXMLFile = new FileInfo(destinationFilePath);

                var masterJobParamXMLFile = new FileInfo(Path.Combine(mWorkDir, AnalysisJob.JobParametersFilename(mJob)));

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
                    mJobParams.AddResultFileToSkip(sourceJobParamXMLFile.Name);
                }

                mJobParams.AddAdditionalParameter(AnalysisJob.JOB_PARAMETERS_SECTION, "PreviousJobStepParameterFile", sourceJobParamXMLFile.Name);
                return success;
            }
            catch (Exception ex)
            {
                LogError("Error in GetExistingJobParametersFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Examine the specified DMS_Temp_Org directory to find the FASTA files and their corresponding .fasta.LastUsed or .hashcheck files
        /// </summary>
        /// <param name="orgDbDirectory">Org DB directory (or parent directory if processing files on a remote share, e.g. \\gigasax\MSGFPlus_Index_Files)</param>
        /// <returns>Dictionary of FASTA files, including the last usage date for each</returns>
        private static Dictionary<FileInfo, DateTime> GetFastaFilesByLastUse(DirectoryInfo orgDbDirectory)
        {
            // Keys are the FASTA file; values are the lastUsed time of the file (nominally obtained from a .hashcheck or .LastUsed file)
            var fastaFiles = new Dictionary<FileInfo, DateTime>();

            var lastProgress = DateTime.UtcNow;
            var longRunning = false;

            foreach (var fastaFile in orgDbDirectory.GetFiles("*" + FASTA_FILE_EXTENSION, SearchOption.AllDirectories))
            {
                if (DateTime.UtcNow.Subtract(lastProgress).TotalSeconds > 1)
                {
                    lastProgress = DateTime.UtcNow;

                    if (!longRunning)
                    {
                        Console.WriteLine("Finding FASTA files below " + orgDbDirectory);
                        longRunning = true;
                    }

                    Console.Write(".");
                }

                if (fastaFiles.ContainsKey(fastaFile))
                    continue;

                var lastUsed = DateMax(fastaFile.LastWriteTimeUtc, fastaFile.CreationTimeUtc);

                if (fastaFile.Directory == null)
                {
                    fastaFiles.Add(fastaFile, lastUsed);
                    continue;
                }

                // Look for a .hashcheck file
                var hashCheckFiles = fastaFile.Directory.GetFiles(fastaFile.Name + "*" + OrganismDatabaseHandler.ProteinExport.GetFASTAFromDMS.HashcheckSuffix).ToList();

                if (hashCheckFiles.Count > 0)
                {
                    lastUsed = DateMax(lastUsed, hashCheckFiles.First().LastWriteTimeUtc);
                }

                // Look for a .LastUsed file
                var lastUsedFiles = fastaFile.Directory.GetFiles(fastaFile.Name + FileSyncUtils.LASTUSED_FILE_EXTENSION).ToList();

                // If this is a .revCat.fasta file, look for .fasta.LastUsed
                if (fastaFile.Name.EndsWith(".revCat.fasta", StringComparison.OrdinalIgnoreCase))
                {
                    var altFastaName = fastaFile.Name.Substring(0, fastaFile.Name.Length - ".revCat.fasta".Length) + ".fasta" + FileSyncUtils.LASTUSED_FILE_EXTENSION;
                    var additionalFiles = fastaFile.Directory.GetFiles(altFastaName).ToList();
                    lastUsedFiles.AddRange(additionalFiles);
                }

                if (lastUsedFiles.Count > 0)
                {
                    lastUsed = DateMax(lastUsed, lastUsedFiles.First().LastWriteTimeUtc);

                    try
                    {
                        // Read the date stored in the file
                        using var lastUsedReader = new StreamReader(new FileStream(lastUsedFiles.First().FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                        if (!lastUsedReader.EndOfStream)
                        {
                            var lastUseDate = lastUsedReader.ReadLine();

                            if (DateTime.TryParse(lastUseDate, out var lastUsedActual))
                            {
                                lastUsed = DateMax(lastUsed, lastUsedActual);
                            }
                        }
                    }
                    catch (Exception)
                    {
                        // Ignore errors here
                    }
                }

                fastaFiles.Add(fastaFile, lastUsed);
            }

            if (longRunning)
                Console.WriteLine();

            return fastaFiles;
        }

        /// <summary>
        /// Converts the given timespan to the total days, hours, minutes, or seconds as a string
        /// </summary>
        /// <param name="timeInterval">Timespan to convert</param>
        /// <returns>Timespan length in human-readable form</returns>
        protected string GetHumanReadableTimeInterval(TimeSpan timeInterval)
        {
            if (timeInterval.TotalDays >= 1)
            {
                // Report Days
                return timeInterval.TotalDays.ToString("0.00") + " days";
            }

            if (timeInterval.TotalHours >= 1)
            {
                // Report hours
                return timeInterval.TotalHours.ToString("0.00") + " hours";
            }

            if (timeInterval.TotalMinutes >= 1)
            {
                // Report minutes
                return timeInterval.TotalMinutes.ToString("0.00") + " minutes";
            }

            // Report seconds
            return timeInterval.TotalSeconds.ToString("0.0") + " seconds";
        }

        /// <summary>
        /// Determine the path to MSFileInfoScanner.dll
        /// </summary>
        /// <param name="mgrParams">Manager parameters</param>
        /// <param name="msFileInfoScannerDLLPath">Output: path to MSFileInfoScanner</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <returns>True if found, false if an error</returns>
        public static bool GetMsFileInfoScannerDllPath(IMgrParams mgrParams, out string msFileInfoScannerDLLPath, out string errorMessage)
        {
            var msFileInfoScannerDir = mgrParams.GetParam("MSFileInfoScannerDir");

            if (string.IsNullOrEmpty(msFileInfoScannerDir))
            {
                errorMessage = "Manager parameter 'MSFileInfoScannerDir' is not defined (GetMsFileInfoScannerDllPath)";
                msFileInfoScannerDLLPath = string.Empty;
                return false;
            }

            msFileInfoScannerDLLPath = Path.Combine(msFileInfoScannerDir, "MSFileInfoScanner.dll");

            if (File.Exists(msFileInfoScannerDLLPath))
            {
                errorMessage = string.Empty;
                return true;
            }

            errorMessage = "File Not Found (GetMsFileInfoScannerDllPath): " + msFileInfoScannerDLLPath;
            return false;
        }

        /// <summary>
        /// Get the MSXML Cache directory path that is appropriate for this job
        /// </summary>
        /// <remarks>Uses job parameter OutputFolderName, which should be something like MSXML_Gen_1_120_275966</remarks>
        /// <param name="cacheDirectoryPathBase">Cache directory path base</param>
        /// <param name="jobParams">Job parameters</param>
        /// <param name="errorMessage">Output: error message</param>
        public static string GetMSXmlCacheFolderPath(string cacheDirectoryPathBase, IJobParams jobParams, out string errorMessage)
        {
            // Lookup the output directory name; e.g. MSXML_Gen_1_120_275966
            var outputDirectoryName = jobParams.GetJobParameter(JOB_PARAM_OUTPUT_FOLDER_NAME, string.Empty);

            if (string.IsNullOrEmpty(outputDirectoryName))
            {
                errorMessage = "outputDirectoryName is empty; cannot construct MSXmlCache path";
                return string.Empty;
            }

            string msXmlToolNameVersionDirectory;

            try
            {
                msXmlToolNameVersionDirectory = GetMSXmlToolNameVersionFolder(outputDirectoryName);
            }
            catch (Exception)
            {
                errorMessage = "outputDirectoryName is not in the expected form of ToolName_Version_DatasetID (" + outputDirectoryName + "); cannot construct MSXmlCache path";
                return string.Empty;
            }

            return GetMSXmlCacheFolderPath(cacheDirectoryPathBase, jobParams, msXmlToolNameVersionDirectory, out errorMessage);
        }

        /// <summary>
        /// Get the path to the cache directory; used for retrieving cached .mzML files that are stored in ToolName_Version directories
        /// </summary>
        /// <remarks>Uses job parameter DatasetStoragePath to determine the Year_Quarter string to append to the end of the path</remarks>
        /// <param name="cacheDirectoryPathBase">Cache directory base, e.g. \\Proto-11\MSXML_Cache</param>
        /// <param name="jobParams">Job parameters</param>
        /// <param name="msXmlToolNameVersionDirectory">ToolName_Version directory, e.g. MSXML_Gen_1_93</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <returns>Path to the cache folder; empty string if an error</returns>
        public static string GetMSXmlCacheFolderPath(
            string cacheDirectoryPathBase,
            IJobParams jobParams,
            string msXmlToolNameVersionDirectory,
            out string errorMessage)
        {
            errorMessage = string.Empty;

            // DatasetStoragePath has the directory (on the storage server) just above the dataset directory
            var datasetStoragePath = jobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "DatasetStoragePath");

            if (string.IsNullOrEmpty(datasetStoragePath))
            {
                datasetStoragePath = jobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "DatasetArchivePath");
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

            // Combine the cache directory path, ToolNameVersion, and the dataset Year_Quarter code
            return Path.Combine(cacheDirectoryPathBase, msXmlToolNameVersionDirectory, yearQuarter);
        }

        /// <summary>
        /// Examine a directory name of the form MSXML_Gen_1_93_367204 and remove the DatasetID or Data Package ID portion
        /// </summary>
        /// <param name="toolNameVersionDatasetIdDirectory">Shared results directory name</param>
        /// <returns>The trimmed directory name if a valid directory; throws an exception if the directory name is not the correct format</returns>
        public static string GetMSXmlToolNameVersionFolder(string toolNameVersionDatasetIdDirectory)
        {
            // Remove the dataset ID or data package ID from the end of the directory name
            var toolNameAndVersionMatcher = new Regex(@"^(?<ToolNameVersion>.+\d+_\d+)_\d+$");
            var match = toolNameAndVersionMatcher.Match(toolNameVersionDatasetIdDirectory);

            if (!match.Success)
            {
                throw new Exception("Directory name is not in the expected form of ToolName_Version_DatasetID; unable to strip out the dataset ID");
            }

            return match.Groups["ToolNameVersion"].ToString();
        }

        /// <summary>
        /// Retrieve the .mzML or .mzXML file associated with this job's dataset (based on Job Parameter MSXMLOutputType)
        /// </summary>
        /// <remarks>
        /// If MSXMLOutputType is not defined, attempts to retrieve a .mzML file
        /// If the .mzML file is not found, the calling method will re-create it (for some plugins)
        /// </remarks>
        /// <returns>Closeout code</returns>
        protected CloseOutType GetMsXmlFile()
        {
            var msXmlOutputType = mJobParams.GetJobParameter("MSXMLOutputType", string.Empty);

            return string.Equals(msXmlOutputType, "mzxml", StringComparison.OrdinalIgnoreCase)
                ? GetMzXMLFile()
                : GetMzMLFile();
        }

        /// <summary>
        /// Retrieve the .mzML file for this dataset
        /// </summary>
        /// <returns>Closeout code</returns>
        public CloseOutType GetMzMLFile()
        {
            LogMessage("Retrieving the mzML file");

            const bool unzipFile = true;

            var success = FileSearchTool.RetrieveCachedMzMLFile(unzipFile, out var errorMessage, out var fileMissingFromCache, out _);

            if (!success)
            {
                return HandleMsXmlRetrieveFailure(fileMissingFromCache, errorMessage, DOT_MZML_EXTENSION);
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Retrieve the .mzXML file for this dataset
        /// </summary>
        /// <remarks>
        /// Do not use RetrieveMZXmlFile since that method looks for any valid MSXML_Gen directory for this dataset
        /// Instead, use FindAndRetrieveMiscFiles
        /// </remarks>
        /// <returns>Closeout code</returns>
        public CloseOutType GetMzXMLFile()
        {
            LogMessage("Retrieving the mzML file");

            // Note that capitalization matters for the extension; it must be .mzXML
            var fileToGet = mDatasetName + DOT_MZXML_EXTENSION;

            if (!FileSearchTool.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Look for a .mzXML file in the cache instead

                const bool unzipFile = true;

                var success = FileSearchTool.RetrieveCachedMzXMLFile(unzipFile, out var errorMessage, out var fileMissingFromCache, out _);

                if (!success)
                {
                    return HandleMsXmlRetrieveFailure(fileMissingFromCache, errorMessage, DOT_MZXML_EXTENSION);
                }
            }
            mJobParams.AddResultFileToSkip(fileToGet);

            // ReSharper disable once RedundantNameQualifier
            if (!mMyEMSLUtilities.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Retrieve the .pbf file for this dataset
        /// </summary>
        protected CloseOutType GetPBFFile()
        {
            LogMessage("Retrieving the PBF file");

            var success = FileSearchTool.RetrieveCachedPBFFile(out var errorMessage, out var fileMissingFromCache, out _);

            if (!success)
            {
                return HandleMsXmlRetrieveFailure(fileMissingFromCache, errorMessage, DOT_PBF_EXTENSION);
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Look for job parameter ResultType
        /// If found, return the value
        /// Otherwise, try to determine the ResultType from the script name
        /// </summary>
        /// <param name="jobParams">Job parameters</param>
        /// <returns>Result type name if found, otherwise an empty string</returns>
        public static string GetResultType(IJobParams jobParams)
        {
            var resultTypeName = jobParams.GetParam("ResultType");

            if (!string.IsNullOrWhiteSpace(resultTypeName))
            {
                return resultTypeName;
            }

            // The ToolName job parameter holds the name of the pipeline script we are executing
            var scriptName = jobParams.GetParam("ToolName");

            if (scriptName.StartsWith("DiaNN", StringComparison.OrdinalIgnoreCase))
            {
                return RESULT_TYPE_DIANN;
            }

            if (scriptName.StartsWith("MaxQuant", StringComparison.OrdinalIgnoreCase))
            {
                return RESULT_TYPE_MAXQUANT;
            }

            if (scriptName.StartsWith("MODa", StringComparison.OrdinalIgnoreCase))
            {
                return RESULT_TYPE_MODA;
            }

            if (scriptName.StartsWith("MODPlus", StringComparison.OrdinalIgnoreCase))
            {
                return RESULT_TYPE_MODPLUS;
            }

            if (scriptName.StartsWith("MSAlign", StringComparison.OrdinalIgnoreCase))
            {
                return RESULT_TYPE_MSALIGN;
            }

            if (scriptName.StartsWith("MSFragger", StringComparison.OrdinalIgnoreCase) ||
                scriptName.StartsWith("FragPipe", StringComparison.OrdinalIgnoreCase))
            {
                return RESULT_TYPE_MSFRAGGER;
            }

            if (scriptName.StartsWith("MSGFPlus", StringComparison.OrdinalIgnoreCase))
            {
                return RESULT_TYPE_MSGFPLUS;
            }

            if (scriptName.StartsWith("MSPathFinder", StringComparison.OrdinalIgnoreCase))
            {
                return RESULT_TYPE_MSPATHFINDER;
            }

            if (scriptName.StartsWith("TopPIC", StringComparison.OrdinalIgnoreCase))
            {
                return RESULT_TYPE_TOPPIC;
            }

            // ReSharper disable once ConvertIfStatementToReturnStatement
            if (scriptName.StartsWith("XTandem", StringComparison.OrdinalIgnoreCase))
            {
                return RESULT_TYPE_XTANDEM;
            }

            return string.Empty;
        }

        /// <summary>
        /// Retrieve the _mgf.zip file for this dataset and extract the .mgf file
        /// </summary>
        /// <remarks>
        /// Do not use RetrieveMZXmlFile since that method looks for any valid MSXML_Gen directory for this dataset
        /// Instead, use FindAndRetrieveMiscFiles
        /// </remarks>
        /// <returns>Closeout code</returns>
        protected CloseOutType GetZippedMgfFile()
        {
            LogMessage("Retrieving the _mgf.zip file");

            var fileToGet = mDatasetName + MGF_ZIPPED_EXTENSION;

            if (!FileSearchTool.FindAndRetrieveMiscFiles(fileToGet, true))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            mJobParams.AddResultFileToSkip(fileToGet);

            // ReSharper disable once RedundantNameQualifier
            if (!mMyEMSLUtilities.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Log an error message when a .mzXML or .mzML file could not be found
        /// </summary>
        /// <param name="fileMissingFromCache">True if the file was missing from the cache</param>
        /// <param name="errorMessage">Error message</param>
        /// <param name="msXmlExtension">File extension</param>
        /// <param name="callerName">Name of the calling method</param>
        protected CloseOutType HandleMsXmlRetrieveFailure(
            bool fileMissingFromCache,
            string errorMessage,
            string msXmlExtension,
            [CallerMemberName] string callerName = "")
        {
            if (fileMissingFromCache)
            {
                if (string.IsNullOrEmpty(errorMessage))
                {
                    errorMessage = "Cached " + msXmlExtension + " file does not exist";
                }

                // msXML file not found in the source directory(s): MSXML_Gen_1_194; will re-generate the .mzML file

                if (!errorMessage.Contains("will re-generate"))
                    errorMessage += "; will re-generate the " + msXmlExtension + " file";

                LogMessage("Warning: " + errorMessage);
                return CloseOutType.CLOSEOUT_FILE_NOT_IN_CACHE;
            }

            if (string.IsNullOrEmpty(errorMessage))
            {
                errorMessage = "Unknown error in " + callerName;
            }

            LogMessage(errorMessage, 0, true);
            return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
        }

        /// <summary>
        /// Get the full path of the parent directory just above directoryPath
        /// </summary>
        /// <param name="directoryPath">Directory path</param>
        private static string GetParentDirectoryPath(string directoryPath)
        {
            var directory = new DirectoryInfo(directoryPath);

            if (directory.Parent != null)
                return directory.Parent.FullName;

            throw new DirectoryNotFoundException("Parent of " + directory.FullName);
        }

        /// <summary>
        /// Gets fake job information for a dataset that is associated with a data package yet has no analysis jobs associated with the data package
        /// </summary>
        /// <param name="datasetInfo">Dataset info</param>
        public static DataPackageJobInfo GetPseudoDataPackageJobInfo(DataPackageDatasetInfo datasetInfo)
        {
            // Store the negative of the dataset ID as the job number
            var pseudoJob = -datasetInfo.DatasetID;
            var jobInfo = new DataPackageJobInfo(pseudoJob, datasetInfo.Dataset)
            {
                DatasetID = datasetInfo.DatasetID,
                Instrument = datasetInfo.Instrument,
                InstrumentGroup = datasetInfo.InstrumentGroup,
                Experiment = datasetInfo.Experiment,
                Experiment_Reason = datasetInfo.Experiment_Reason,
                Experiment_Comment = datasetInfo.Experiment_Comment,
                Experiment_Organism = datasetInfo.Experiment_Organism,
                Experiment_NEWT_ID = datasetInfo.Experiment_NEWT_ID,
                Experiment_NEWT_Name = datasetInfo.Experiment_NEWT_Name,
                Tool = "Dataset info (no tool)",
                NumberOfClonedSteps = 0,
                ResultType = "Dataset info (no type)",
                PeptideHitResultType = PeptideHitResultTypes.Unknown,
                SettingsFileName = string.Empty,
                ParameterFileName = string.Empty,
                GeneratedFASTAFileName = string.Empty,
                LegacyFastaFileName = string.Empty,
                ProteinCollectionList = string.Empty,
                ProteinOptions = string.Empty,
                ResultsFolderName = string.Empty,
                DatasetFolderName = datasetInfo.Dataset,
                RawDataType = datasetInfo.RawDataType
            };

            jobInfo.SharedResultsFolders.Clear();

            // In jobInfo, ServerStoragePath and ArchiveStoragePath track the directory just above the dataset directory
            // In contrast, in datasetInfo, DatasetDirectoryPath and DatasetArchivePath track the actual dataset directory path

            try
            {
                jobInfo.ServerStoragePath = GetParentDirectoryPath(datasetInfo.DatasetDirectoryPath);
            }
            catch (Exception)
            {
                LogTools.LogWarning("Error in GetPseudoDataPackageJobInfo determining the parent directory of " + datasetInfo.DatasetDirectoryPath);
                jobInfo.ServerStoragePath = datasetInfo.DatasetDirectoryPath.Replace(@"\" + datasetInfo.Dataset, string.Empty);
            }

            try
            {
                jobInfo.ArchiveStoragePath = string.IsNullOrWhiteSpace(datasetInfo.DatasetArchivePath)
                    ? string.Empty
                    : GetParentDirectoryPath(datasetInfo.DatasetArchivePath);
            }
            catch (Exception)
            {
                LogTools.LogWarning("Error in GetPseudoDataPackageJobInfo determining the parent directory of " + datasetInfo.DatasetArchivePath);
                jobInfo.ArchiveStoragePath = datasetInfo.DatasetArchivePath.Replace(@"\" + datasetInfo.Dataset, string.Empty);
            }

            return jobInfo;
        }

        /// <summary>
        /// Convert a raw data type string to raw data type enum (i.e. instrument data type)
        /// </summary>
        /// <param name="rawDataTypeName">Raw data type name</param>
        public static RawDataTypeConstants GetRawDataType(string rawDataTypeName)
        {
            if (string.IsNullOrEmpty(rawDataTypeName))
            {
                return RawDataTypeConstants.Unknown;
            }

            return rawDataTypeName.ToLower() switch
            {
                RAW_DATA_TYPE_DOT_D_FOLDERS => RawDataTypeConstants.AgilentDFolder,
                RAW_DATA_TYPE_ZIPPED_S_FOLDERS => RawDataTypeConstants.ZippedSFolders,
                RAW_DATA_TYPE_DOT_RAW_FOLDER => RawDataTypeConstants.MicromassRawFolder,
                RAW_DATA_TYPE_DOT_RAW_FILES => RawDataTypeConstants.ThermoRawFile,
                RAW_DATA_TYPE_DOT_WIFF_FILES => RawDataTypeConstants.AgilentQStarWiffFile,
                RAW_DATA_TYPE_DOT_UIMF_FILES => RawDataTypeConstants.UIMF,
                RAW_DATA_TYPE_DOT_MZXML_FILES => RawDataTypeConstants.mzXML,
                RAW_DATA_TYPE_DOT_MZML_FILES => RawDataTypeConstants.mzML,
                RAW_DATA_TYPE_BRUKER_FT_FOLDER => RawDataTypeConstants.BrukerFTFolder,
                RAW_DATA_TYPE_BRUKER_MALDI_SPOT => RawDataTypeConstants.BrukerMALDISpot,
                RAW_DATA_TYPE_BRUKER_MALDI_IMAGING => RawDataTypeConstants.BrukerMALDIImaging,
                RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER => RawDataTypeConstants.BrukerTOFBaf,
                RAW_DATA_TYPE_BRUKER_TOF_TDF_FOLDER => RawDataTypeConstants.BrukerTOFTdf,
                _ => RawDataTypeConstants.Unknown
            };
        }

        /// <summary>
        /// Determine the raw data type (i.e. instrument data type)
        /// </summary>
        /// <param name="jobParams">Job parameters</param>
        /// <param name="errorMessage">Output: error message</param>
        public static string GetRawDataTypeName(IJobParams jobParams, out string errorMessage)
        {
            errorMessage = string.Empty;

            var msXmlOutputType = jobParams.GetParam("MSXMLOutputType");

            if (string.IsNullOrWhiteSpace(msXmlOutputType))
            {
                return jobParams.GetParam("RawDataType");
            }

            return msXmlOutputType.ToLower() switch
            {
                "mzxml" => RAW_DATA_TYPE_DOT_MZXML_FILES,
                "mzml" => RAW_DATA_TYPE_DOT_MZML_FILES,
                _ => string.Empty
            };
        }

        /// <summary>
        /// Determine the RawDataType name for this job
        /// </summary>
        protected string GetRawDataTypeName()
        {
            var rawDataTypeName = GetRawDataTypeName(mJobParams, out var errorMessage);

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
        /// Get the name of the split FASTA file to use for this job
        /// </summary>
        /// <remarks>Returns an empty string if an error</remarks>
        /// <param name="jobParams">Job parameters</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <returns>The name of the split FASTA file to use</returns>
        public static string GetSplitFastaFileName(IJobParams jobParams, out string errorMessage)
        {
            return GetSplitFastaFileName(jobParams, out errorMessage, out _);
        }

        /// <summary>
        /// Get the name of the split FASTA file to use for this job
        /// </summary>
        /// <remarks>Returns an empty string if an error</remarks>
        /// <param name="jobParams">Job parameters</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <param name="numberOfClonedSteps">Output: total number of cloned steps</param>
        /// <returns>The name of the split FASTA file to use</returns>
        public static string GetSplitFastaFileName(IJobParams jobParams, out string errorMessage, out int numberOfClonedSteps)
        {
            numberOfClonedSteps = 0;

            var legacyFastaFileName = jobParams.GetJobParameter("LegacyFastaFileName", string.Empty);

            if (string.IsNullOrEmpty(legacyFastaFileName))
            {
                errorMessage = "Parameter LegacyFastaFileName is empty for the job; cannot determine the SplitFasta file name for this job step";
                LogTools.LogError(errorMessage);
                return string.Empty;
            }

            numberOfClonedSteps = jobParams.GetJobParameter("NumberOfClonedSteps", 0);

            if (numberOfClonedSteps == 0)
            {
                errorMessage = "Settings file is missing parameter NumberOfClonedSteps; cannot determine the SplitFasta file name for this job step";
                LogTools.LogError(errorMessage);
                return string.Empty;
            }

            var iteration = GetSplitFastaIteration(jobParams, out errorMessage);

            if (iteration < 1)
            {
                var toolName = jobParams.GetJobParameter("StepTool", string.Empty);

                if (Global.IsMatch(toolName, "Mz_Refinery"))
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
                        LogTools.LogError(errorMessage);
                    }
                    return string.Empty;
                }
            }

            var fastaNameBase = Path.GetFileNameWithoutExtension(legacyFastaFileName);
            var splitFastaName = fastaNameBase + "_" + numberOfClonedSteps + "x_";

            if (numberOfClonedSteps < 10)
            {
                splitFastaName += iteration.ToString("0") + FASTA_FILE_EXTENSION;
            }
            else if (numberOfClonedSteps < 100)
            {
                splitFastaName += iteration.ToString("00") + FASTA_FILE_EXTENSION;
            }
            else
            {
                splitFastaName += iteration.ToString("000") + FASTA_FILE_EXTENSION;
            }

            return splitFastaName;
        }

        /// <summary>
        /// Compute the split FASTA iteration for the current job step
        /// </summary>
        /// <param name="jobParams">Job parameters</param>
        /// <param name="errorMessage">Output: error message</param>
        public static int GetSplitFastaIteration(IJobParams jobParams, out string errorMessage)
        {
            errorMessage = string.Empty;

            var cloneStepRenumberStart = jobParams.GetJobParameter("CloneStepRenumberStart", 0);

            if (cloneStepRenumberStart == 0)
            {
                errorMessage = "Settings file is missing parameter CloneStepRenumberStart; cannot determine the SplitFasta iteration value for this job step";
                LogTools.LogError(errorMessage);
                return 0;
            }

            var stepNumber = jobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, "Step", 0);

            if (stepNumber == 0)
            {
                errorMessage = "Job parameter Step is missing; cannot determine the SplitFasta iteration value for this job step";
                LogTools.LogError(errorMessage);
                return 0;
            }

            return stepNumber - cloneStepRenumberStart + 1;
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

                if (!string.Equals(nameAttrib.Value, AnalysisJob.STEP_PARAMETERS_SECTION, StringComparison.OrdinalIgnoreCase))
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
        /// Get the input or output transfer directory path specific to this job step
        /// </summary>
        /// <param name="useInputDirectory">True to use "InputFolderName", false to use "OutputFolderName"</param>
        /// <param name="includeDatasetName">When true, insert the dataset name between the base transfer directory path and the job directory</param>
        protected string GetTransferDirectoryPathForJobStep(bool useInputDirectory, bool includeDatasetName = true)
        {
            return GetTransferDirectoryPathForJobStep(
                mJobParams, useInputDirectory,
                out _, out _,
                includeDatasetName, mDatasetName);
        }

        /// <summary>
        /// Get the input or output transfer directory path specific to this job step
        /// </summary>
        /// <param name="jobParams">Job parameters</param>
        /// <param name="useInputDirectory">True to use "InputFolderName", false to use "OutputFolderName"</param>
        /// <param name="missingJobParamTransferDirectoryPath">Output: true if job parameter "TransferFolderPath" is not defined</param>
        /// <param name="missingJobParamResultsDirectoryName">Output: true if job parameter "InputFolderName" or "OutputFolderName" is not defined</param>
        /// <param name="includeDatasetName">When true, insert the dataset name between the base transfer directory path and the job directory</param>
        /// <param name="datasetName">Dataset name</param>
        public static string GetTransferDirectoryPathForJobStep(
            IJobParams jobParams,
            bool useInputDirectory,
            out bool missingJobParamTransferDirectoryPath,
            out bool missingJobParamResultsDirectoryName,
            bool includeDatasetName = true,
            string datasetName = "")
        {
            var transferDirPathBase = jobParams.GetParam(JOB_PARAM_TRANSFER_DIRECTORY_PATH);

            if (string.IsNullOrEmpty(transferDirPathBase))
            {
                // Transfer directory parameter is empty; return an empty string
                missingJobParamTransferDirectoryPath = true;
                missingJobParamResultsDirectoryName = false;
                return string.Empty;
            }

            missingJobParamTransferDirectoryPath = false;

            string datasetDirectoryName;

            if (includeDatasetName)
            {
                // Append the dataset directory name (or the dataset name) to the transfer directory path
                var jobParamsDatasetDirectoryName = jobParams.GetParam(AnalysisJob.STEP_PARAMETERS_SECTION, JOB_PARAM_DATASET_FOLDER_NAME);
                datasetDirectoryName = string.IsNullOrWhiteSpace(jobParamsDatasetDirectoryName) ? datasetName : jobParamsDatasetDirectoryName;
            }
            else
            {
                datasetDirectoryName = string.Empty;
            }

            string directoryName;

            if (useInputDirectory)
            {
                directoryName = jobParams.GetParam(JOB_PARAM_INPUT_FOLDER_NAME);
            }
            else
            {
                directoryName = jobParams.GetParam(JOB_PARAM_OUTPUT_FOLDER_NAME);
            }

            if (string.IsNullOrEmpty(directoryName))
            {
                // Input (or output) directory parameter is empty; return an empty string
                missingJobParamResultsDirectoryName = true;
                return string.Empty;
            }

            missingJobParamResultsDirectoryName = false;
            return Path.Combine(transferDirPathBase, datasetDirectoryName, directoryName);
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
            if (string.IsNullOrWhiteSpace(mWorkDir))
                yield break;

            var workDir = new DirectoryInfo(mWorkDir);

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
        /// <returns>True if success, false if an error</returns>
        protected bool GUnzipFile(string gzipFilePath)
        {
            return FileSearchTool.GUnzipFile(gzipFilePath);
        }

        /// <summary>
        /// Return true if the dataset name is of the form "DataPackage_1234_"
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        public static bool IsDataPackageDataset(string datasetName)
        {
            var dataPackageMatcher = new Regex(@"^DataPackage_\d+_", RegexOptions.IgnoreCase);

            return dataPackageMatcher.IsMatch(datasetName);
        }

        // ReSharper disable once CommentTypo

        /// <summary>
        /// Looks up dataset information for a data package
        /// </summary>
        /// <remarks>
        /// The dataPackageDatasets dictionary will be empty if this job is not associated with a data package
        /// </remarks>
        /// <param name="dataPackageDatasets">Output: datasets associated with the given data package; keys are DatasetID</param>
        /// <param name="dataPackageSharePath">Output: data package share path, e.g. \\protoapps\DataPkgs\Public\2024\5998_CPTAC_CompRef_Acetyl</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <param name="logErrors">Log errors if true (default)</param>
        /// <returns>True if a data package is defined and has datasets associated with it, otherwise false</returns>
        protected bool LoadDataPackageDatasetInfo(
            out Dictionary<int, DataPackageDatasetInfo> dataPackageDatasets,
            out string dataPackageSharePath,
            out string errorMessage,
            bool logErrors)
        {
            // SQL Server: Data Source=Gigasax;Initial Catalog=DMS_Pipeline
            // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms
            var connectionString = mMgrParams.GetParam("BrokerConnectionString");

            var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);

            if (dataPackageID <= 0)
            {
                dataPackageDatasets = new Dictionary<int, DataPackageDatasetInfo>();
                dataPackageSharePath = string.Empty;
                errorMessage = string.Empty;
                return false;
            }

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, mMgrName);

            var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: TraceMode);
            RegisterEvents(dbTools);

            var success = DataPackageInfoLoader.GetDataPackageSharePath(this, dbTools, dataPackageID, out dataPackageSharePath, out var errorMessage1, logErrors);

            // ReSharper disable once InvertIf
            if (!success)
            {
                dataPackageDatasets = new Dictionary<int, DataPackageDatasetInfo>();
                errorMessage = errorMessage1;
                return false;
            }

            return DataPackageInfoLoader.LoadDataPackageDatasetInfo(this, dbTools, dataPackageID, out dataPackageDatasets, out errorMessage, logErrors);
        }

        /// <summary>
        /// Looks up dataset information for the data package associated with this analysis job
        /// </summary>
        /// <remarks>
        /// Property NumberOfClonedSteps is not updated for the analysis jobs returned by this method
        /// In contrast, RetrieveDataPackagePeptideHitJobInfo does update NumberOfClonedSteps
        /// </remarks>
        /// <param name="dataPackageJobs">Output: dictionary tracking data package jobs; keys are job numbers, values are job info</param>
        /// <returns>True if a data package is defined and has analysis jobs associated with it, otherwise false</returns>
        private bool LoadDataPackageJobInfo(out Dictionary<int, DataPackageJobInfo> dataPackageJobs)
        {
            // SQL Server: Data Source=Gigasax;Initial Catalog=DMS_Pipeline
            // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms
            var connectionString = mMgrParams.GetParam("BrokerConnectionString");

            var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);

            if (dataPackageID <= 0)
            {
                dataPackageJobs = new Dictionary<int, DataPackageJobInfo>();
                return false;
            }

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, mMgrName);

            var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: TraceMode);
            RegisterEvents(dbTools);

            return DataPackageInfoLoader.LoadDataPackageJobInfo(dbTools, dataPackageID, out dataPackageJobs);
        }

        /// <summary>
        /// Log a debug message
        /// </summary>
        /// <param name="debugMessage">Debug message</param>
        protected void LogDebugMessage(string debugMessage)
        {
            LogTools.LogDebug(debugMessage);
        }

        /// <summary>
        /// Log a debug message
        /// </summary>
        /// <param name="debugMessage">Debug message</param>
        /// <param name="statusTools">Status tools instance</param>
        protected static void LogDebugMessage(string debugMessage, IStatusFile statusTools)
        {
            LogTools.LogDebug(debugMessage);

            if (statusTools != null)
            {
                statusTools.CurrentOperation = debugMessage;
                statusTools.UpdateAndWrite(0);
            }
        }

        /// <summary>
        /// Retrieve the metadata for the datasets associated with the given data package
        /// </summary>
        /// <param name="dataPackageID">Data package ID</param>
        /// <param name="datasetIDsByExperimentGroup">
        /// Output: dictionary where keys are experiment group name and values are dataset ID (this is used by MSFragger)
        /// </param>
        /// <param name="dataPackageError">Output: true if a database error or the data package has a placeholder dataset</param>
        /// <param name="storeJobParameters">When true, store the data package info as packed dictionary job parameters</param>
        /// <returns>True if the data package is defined and it has datasets associated with it</returns>
        protected bool LookupDataPackageInfo(
            int dataPackageID,
            out SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            out bool dataPackageError,
            bool storeJobParameters)
        {
            try
            {
                // SQL Server: Data Source=Gigasax;Initial Catalog=DMS_Pipeline
                // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms
                var brokerDbConnectionString = mMgrParams.GetParam("BrokerConnectionString");

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(brokerDbConnectionString, mMgrName);

                var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: TraceMode);
                RegisterEvents(dbTools);

                var dataPackageFileHandler = new DataPackageFileHandler(dbTools, dataPackageID, this);
                RegisterEvents(dataPackageFileHandler);

                var dataPackageInfoLoader = new DataPackageInfoLoader(this, dbTools, dataPackageID);

                var success = dataPackageInfoLoader.LoadDataPackageDatasetInfo(out var dataPackageDatasets, out var errorMessage, false);

                if (success && dataPackageDatasets.Count > 0)
                {
                    datasetIDsByExperimentGroup = DataPackageInfoLoader.GetDataPackageDatasetsByExperimentGroup(dataPackageDatasets);

                    if (storeJobParameters)
                    {
                        var dataPackageInfo = new DataPackageInfo(dataPackageID, dataPackageDatasets);
                        RegisterEvents(dataPackageInfo);

                        dataPackageInfo.StorePackedDictionaries(this);
                    }

                    dataPackageError = false;
                    return true;
                }

                if (string.IsNullOrWhiteSpace(errorMessage))
                {
                    LogError("Did not find any datasets associated with this job's data package (ID {0})", dataPackageInfoLoader.DataPackageID);
                    dataPackageError = false;
                }
                else
                {
                    LogError(errorMessage);
                    dataPackageError = true;
                }

                datasetIDsByExperimentGroup = new SortedDictionary<string, SortedSet<int>>(StringComparer.OrdinalIgnoreCase);
                return false;
            }
            catch (Exception ex)
            {
                LogError("Error in LookupDataPackageInfo calling LoadDataPackageDatasetInfo", ex);
                dataPackageError = true;

                datasetIDsByExperimentGroup = new SortedDictionary<string, SortedSet<int>>(StringComparer.OrdinalIgnoreCase);
                return false;
            }
        }

        /// <summary>
        /// Retrieve the information for the specified analysis job
        /// </summary>
        /// <remarks>This procedure is used by AnalysisResourcesQCART</remarks>
        /// <param name="jobNumber">Job number</param>
        /// <param name="jobInfo">Output: Job Info</param>
        /// <returns>True if success, false if an error</returns>
        protected bool LookupJobInfo(int jobNumber, out DataPackageJobInfo jobInfo)
        {
            var sqlStr = new StringBuilder();

            // This query uses view V_Analysis_Job_Export_DataPkg in the DMS5 database

            // Query results are parsed using method DataPackageInfoLoader.ParseDataPackageJobInfoRow, which is
            // designed for view V_DMS_Data_Package_Aggregation_Jobs, but view V_Analysis_Job_Export_DataPkg has identical column names

            sqlStr.Append("SELECT job, dataset, dataset_id, instrument_name, instrument_group,");
            sqlStr.Append("       experiment, experiment_reason, experiment_comment, organism, experiment_newt_id, experiment_newt_name,");
            sqlStr.Append("       tool, result_type, settings_file_name, parameter_file_name,");
            sqlStr.Append("       organism_db_name, protein_collection_list, protein_options,");
            sqlStr.Append("       server_storage_path, archive_storage_path, results_folder, dataset_folder,");
            sqlStr.Append("       1 as step, '' as shared_results_folder, raw_data_type");
            sqlStr.Append("FROM V_Analysis_Job_Export_DataPkg ");
            sqlStr.Append("WHERE job = " + jobNumber);

            var genericJobInfo = new DataPackageJobInfo(0, string.Empty);

            // SQL Server: Data Source=Gigasax;Initial Catalog=DMS5
            // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms
            var dmsConnectionString = mMgrParams.GetParam("ConnectionString");

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(dmsConnectionString, mMgrName);

            var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: TraceMode);
            RegisterEvents(dbTools);

            var success = dbTools.GetQueryResultsDataTable(sqlStr.ToString(), out var resultSet);

            if (!success)
            {
                const string errorMessage = "LookupJobInfo; Excessive failures attempting to retrieve data package job info from database";
                LogMessage(errorMessage, 0, true);
                jobInfo = genericJobInfo;
                return false;
            }

            // Verify at least one row returned
            if (resultSet.Rows.Count < 1)
            {
                // No data was returned
                LogError("Job " + jobNumber + " not found in view V_Analysis_Job_Export_DataPkg");
                jobInfo = genericJobInfo;
                return false;
            }

            jobInfo = DataPackageInfoLoader.ParseDataPackageJobInfoRow(resultSet.Rows[0]);

            return true;
        }

        /// <summary>
        /// Estimate the amount of disk space required for the FASTA file associated with this analysis job
        /// Includes the expected disk usage of index files
        /// </summary>
        /// <remarks>
        /// Uses both mJobParams and mMgrParams
        /// </remarks>
        /// <param name="proteinCollectionInfo">Collection info object</param>
        /// <param name="legacyFastaName">
        /// Output: the FASTA file name
        /// For split FASTA searches, will be the original FASTA file if running extraction,
        /// or if running MS-GF+ (or similar), the split FASTA file corresponding to this job step</param>
        /// <param name="fastaFileSizeGB">Output: FASTA file size, in GB</param>
        /// <returns>Space required, in MB, or 0 if a problem (e.g. the legacy FASTA file is not listed in V_Organism_DB_File_Export)</returns>
        public double LookupLegacyDBDiskSpaceRequiredMB(
            ProteinCollectionInfo proteinCollectionInfo,
            out string legacyFastaName,
            out double fastaFileSizeGB)
        {
            legacyFastaName = string.Empty;
            fastaFileSizeGB = 0;

            try
            {
                var dmsConnectionString = mMgrParams.GetParam("ConnectionString");

                if (string.IsNullOrWhiteSpace(dmsConnectionString))
                {
                    LogError("Error in LookupLegacyDBDiskSpaceRequiredMB: manager parameter ConnectionString is not defined");
                    return 0;
                }

                if (proteinCollectionInfo.UsingSplitFasta && !RunningDataExtraction)
                {
                    legacyFastaName = GetSplitFastaFileName(mJobParams, out _);
                }
                else
                {
                    legacyFastaName = proteinCollectionInfo.LegacyFastaName;
                }

                if (string.IsNullOrWhiteSpace(legacyFastaName))
                    return 0;

                var sqlQuery = "SELECT file_size_kb FROM V_Organism_DB_File_Export WHERE (filename = '" + legacyFastaName + "')";

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(dmsConnectionString, mMgrName);

                var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: TraceMode);
                RegisterEvents(dbTools);

                // Obtain results, as a list of columns (first row only if multiple rows)
                var success = Global.GetQueryResultsTopRow(dbTools, sqlQuery, out var legacyDbSize);

                if (!success || legacyDbSize == null || legacyDbSize.Count == 0)
                {
                    // Empty query results
                    var statusMessage = "Warning: Could not determine the legacy FASTA file's size for job " + mJob + ", file " + legacyFastaName;

                    if (proteinCollectionInfo.UsingSplitFasta)
                    {
                        // Likely the FASTA file has not yet been split
                        LogMessage(statusMessage + "; likely the split FASTA file has not yet been created");
                    }
                    else
                    {
                        LogMessage(statusMessage, 0, true);
                    }

                    return 0;
                }

                if (!int.TryParse(legacyDbSize.First(), out var fileSizeKB))
                {
                    var logMessage = "Legacy FASTA file size is not numeric, job " + mJob + ", file " + legacyFastaName + ": " + legacyDbSize.First();
                    LogMessage(logMessage, 0, true);
                    return 0;
                }

                fastaFileSizeGB = fileSizeKB / 1024.0 / 1024;

                // Assume that the MS-GF+ index files will be 15 times larger than the legacy FASTA file itself
                var fileSizeMB = (fileSizeKB + fileSizeKB * 15) / 1024.0;

                // Pad the expected size by an additional 15%
                return fileSizeMB * 1.15;
            }
            catch (Exception ex)
            {
                LogError("Error in LookupLegacyDBDiskSpaceRequiredMB", ex);
                return 0;
            }
        }

        /// <summary>
        /// Look for StepParameter section entries in the source file and insert them into the master file
        /// If the master file already has information on the given job step, information for that step is not copied from the source
        /// This is because the master file is assumed to be newer than the source file
        /// </summary>
        /// <param name="sourceJobParamXMLFilePath">Source job parameter XML file</param>
        /// <param name="masterJobParamXMLFilePath">Job parameter XML file to update</param>
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

                using var fileWriter = new StreamWriter(new FileStream(masterJobParamXMLFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite));
                using var writer = XmlWriter.Create(fileWriter, settings);

                masterDoc.Save(writer);

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in MergeJobParamXMLStepParameters", ex);
                return false;
            }
        }

        /// <summary>
        /// Moves a file from one directory to another directory
        /// </summary>
        /// <param name="sourceDirectory">Source directory</param>
        /// <param name="targetDirectory">Target directory</param>
        /// <param name="sourceFileName">Source file name</param>
        protected void MoveFileToFolder(DirectoryInfo sourceDirectory, DirectoryInfo targetDirectory, string sourceFileName)
        {
            var sourceFile = new FileInfo(Path.Combine(sourceDirectory.FullName, sourceFileName));
            var targetFilePath = Path.Combine(targetDirectory.FullName, sourceFileName);
            sourceFile.MoveTo(targetFilePath);
        }

        /// <summary>
        /// Override current dataset information, including dataset name, dataset ID, and storage paths
        /// </summary>
        /// <param name="dataPkgDataset">Data package dataset</param>
        public bool OverrideCurrentDatasetInfo(DataPackageDatasetInfo dataPkgDataset)
        {
            if (string.IsNullOrEmpty(dataPkgDataset.Dataset))
            {
                LogError("OverrideCurrentDatasetInfo; Column 'Dataset' not defined for dataset " + dataPkgDataset.Dataset + " in the data package");
                return false;
            }

            var aggregationJob = Global.IsMatch(dataPkgDataset.Dataset, AGGREGATION_JOB_DATASET) || IsDataPackageDataset(dataPkgDataset.Dataset);

            if (!aggregationJob)
            {
                // Update job params to have the details for the current dataset
                // This is required so that we can use FindDataFile to find the desired files
                if (string.IsNullOrEmpty(dataPkgDataset.DatasetDirectoryPath))
                {
                    LogError("OverrideCurrentDatasetInfo; Column 'ServerStoragePath' not defined for dataset " + dataPkgDataset.Dataset + " in the data package");
                    return false;
                }

                if (string.IsNullOrEmpty(dataPkgDataset.DatasetArchivePath))
                {
                    if (Environment.MachineName.StartsWith("CBDMS", StringComparison.OrdinalIgnoreCase))
                    {
                        LogMessage("OverrideCurrentDatasetInfo; Column 'ArchiveStoragePath' not defined for dataset " + dataPkgDataset.Dataset + " in the data package; this is normal for CBDMS");
                    }
                    else
                    {
                        LogWarning("OverrideCurrentDatasetInfo; Column 'ArchiveStoragePath' not defined for dataset " + dataPkgDataset.Dataset + " in the data package");
                    }
                }
            }

            mJobParams.AddDatasetInfo(dataPkgDataset.Dataset, dataPkgDataset.DatasetID);
            DatasetName = dataPkgDataset.Dataset;

            const string jobParamsSection = AnalysisJob.JOB_PARAMETERS_SECTION;

            mJobParams.AddAdditionalParameter(jobParamsSection, JOB_PARAM_DATASET_NAME, dataPkgDataset.Dataset);
            mJobParams.AddAdditionalParameter(jobParamsSection, "DatasetID", dataPkgDataset.DatasetID.ToString());
            mJobParams.AddAdditionalParameter(jobParamsSection, "DatasetType", dataPkgDataset.DatasetType);

            mJobParams.AddAdditionalParameter(jobParamsSection, "Instrument", dataPkgDataset.Instrument);
            mJobParams.AddAdditionalParameter(jobParamsSection, "InstrumentGroup", dataPkgDataset.InstrumentGroup);

            // In the job parameters, ServerStoragePath and ArchiveStoragePath track the directory just above the dataset directory
            // In contrast, in datasetInfo, DatasetDirectoryPath and DatasetArchivePath track the actual dataset directory path

            mJobParams.AddAdditionalParameter(jobParamsSection, "DatasetStoragePath", GetParentDirectoryPath(dataPkgDataset.DatasetDirectoryPath));

            var datasetArchivePath = string.IsNullOrWhiteSpace(dataPkgDataset.DatasetArchivePath)
                ? string.Empty
                : GetParentDirectoryPath(dataPkgDataset.DatasetArchivePath);

            mJobParams.AddAdditionalParameter(jobParamsSection, "DatasetArchivePath", datasetArchivePath);

            mJobParams.AddAdditionalParameter(jobParamsSection, JOB_PARAM_DATASET_FOLDER_NAME, dataPkgDataset.Dataset);
            mJobParams.AddAdditionalParameter(jobParamsSection, "RawDataType", dataPkgDataset.RawDataType);

            return true;
        }

        /// <summary>
        /// Override current job information, including dataset name, dataset ID, storage paths, Organism Name, Protein Collection, and protein options
        /// </summary>
        /// <remarks> Does not override the job number</remarks>
        /// <param name="dataPkgJob">Data package job info</param>
        public bool OverrideCurrentDatasetAndJobInfo(DataPackageJobInfo dataPkgJob)
        {
            if (string.IsNullOrEmpty(dataPkgJob.Dataset))
            {
                LogError("OverrideCurrentDatasetAndJobInfo; Column 'Dataset' not defined for job " + dataPkgJob.Job + " in the data package");
                return false;
            }

            var aggregationJob = Global.IsMatch(dataPkgJob.Dataset, AGGREGATION_JOB_DATASET) || IsDataPackageDataset(dataPkgJob.Dataset);

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
                    if (Environment.MachineName.StartsWith("CBDMS", StringComparison.OrdinalIgnoreCase))
                    {
                        LogMessage("OverrideCurrentDatasetAndJobInfo; Column 'ArchiveStoragePath' not defined for job " + dataPkgJob.Job + " in the data package; this is normal for CBDMS");
                    }
                    else
                    {
                        LogWarning("OverrideCurrentDatasetAndJobInfo; Column 'ArchiveStoragePath' not defined for job " + dataPkgJob.Job + " in the data package");
                    }
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

            mJobParams.AddDatasetInfo(dataPkgJob.Dataset, dataPkgJob.DatasetID);
            DatasetName = dataPkgJob.Dataset;

            const string jobParamsSection = AnalysisJob.JOB_PARAMETERS_SECTION;
            const string peptideSearchSection = AnalysisJob.PEPTIDE_SEARCH_SECTION;

            mJobParams.AddAdditionalParameter(jobParamsSection, JOB_PARAM_DATASET_NAME, dataPkgJob.Dataset);
            mJobParams.AddAdditionalParameter(jobParamsSection, "DatasetID", dataPkgJob.DatasetID.ToString());

            mJobParams.AddAdditionalParameter(jobParamsSection, "Instrument", dataPkgJob.Instrument);
            mJobParams.AddAdditionalParameter(jobParamsSection, "InstrumentGroup", dataPkgJob.InstrumentGroup);

            mJobParams.AddAdditionalParameter(jobParamsSection, "NumberOfClonedSteps", dataPkgJob.NumberOfClonedSteps.ToString());

            mJobParams.AddAdditionalParameter(jobParamsSection, "ToolName", dataPkgJob.Tool);
            mJobParams.AddAdditionalParameter(jobParamsSection, "ResultType", dataPkgJob.ResultType);
            mJobParams.AddAdditionalParameter(jobParamsSection, "SettingsFileName", dataPkgJob.SettingsFileName);

            mJobParams.AddAdditionalParameter(peptideSearchSection, JOB_PARAM_PARAMETER_FILE, dataPkgJob.ParameterFileName);

            mJobParams.AddAdditionalParameter(peptideSearchSection, JOB_PARAM_GENERATED_FASTA_NAME, dataPkgJob.GeneratedFASTAFileName);
            mJobParams.AddAdditionalParameter(peptideSearchSection, "LegacyFastaFileName", dataPkgJob.LegacyFastaFileName);
            mJobParams.AddAdditionalParameter(peptideSearchSection, "ProteinCollectionList", dataPkgJob.ProteinCollectionList);
            mJobParams.AddAdditionalParameter(peptideSearchSection, "ProteinOptions", dataPkgJob.ProteinOptions);

            mJobParams.AddAdditionalParameter(jobParamsSection, "DatasetStoragePath", dataPkgJob.ServerStoragePath);
            mJobParams.AddAdditionalParameter(jobParamsSection, "DatasetArchivePath", dataPkgJob.ArchiveStoragePath);

            mJobParams.AddAdditionalParameter(jobParamsSection, JOB_PARAM_INPUT_FOLDER_NAME, dataPkgJob.ResultsFolderName);
            mJobParams.AddAdditionalParameter(jobParamsSection, JOB_PARAM_DATASET_FOLDER_NAME, dataPkgJob.DatasetFolderName);
            mJobParams.AddAdditionalParameter(jobParamsSection, JOB_PARAM_SHARED_RESULTS_FOLDERS, string.Join(",", dataPkgJob.SharedResultsFolders));
            mJobParams.AddAdditionalParameter(jobParamsSection, "RawDataType", dataPkgJob.RawDataType);

            return true;
        }

        /// <summary>
        /// Download any queued files from MyEMSL
        /// </summary>
        public bool ProcessMyEMSLDownloadQueue()
        {
            if (mMyEMSLUtilities.FilesToDownload.Count > 0)
            {
                // ReSharper disable once RedundantNameQualifier
                if (!mMyEMSLUtilities.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Download any queued files from MyEMSL
        /// </summary>
        /// <param name="targetDirectoryPath">Target directory path</param>
        /// <param name="directoryLayout">Directory layout enum</param>
        protected bool ProcessMyEMSLDownloadQueue(string targetDirectoryPath, Downloader.DownloadLayout directoryLayout)
        {
            if (Global.OfflineMode)
                return true;

            return mMyEMSLUtilities.ProcessMyEMSLDownloadQueue(targetDirectoryPath, directoryLayout);
        }

        /// <summary>
        /// Delete the specified FASTA file and its associated files
        /// </summary>
        /// <param name="fileToPurge">FASTA file to delete</param>
        /// <param name="legacyFastaFileBaseName">Legacy FASTA file base name</param>
        /// <param name="debugLevel">Debug Level for logging; 1=minimal logging; 5=detailed logging</param>
        /// <param name="preview">If true, show the files that would be deleted</param>
        /// <returns>Number of bytes deleted</returns>
        private static long PurgeFastaFiles(FileInfo fileToPurge, string legacyFastaFileBaseName, short debugLevel, bool preview)
        {
            var baseName = Path.GetFileNameWithoutExtension(fileToPurge.Name);

            if (!string.IsNullOrWhiteSpace(legacyFastaFileBaseName) && baseName.StartsWith(legacyFastaFileBaseName, StringComparison.OrdinalIgnoreCase))
            {
                // The current job needs this file; do not delete it
                return 0;
            }

            // ReSharper disable StringLiteralTypo

            // Remove additional text from the base name if present
            var extensionsToTrim = new List<string> {
                ".revcat",
                ".icsfldecoy"
            };

            // ReSharper restore StringLiteralTypo

            foreach (var item in extensionsToTrim)
            {
                if (baseName.EndsWith(item, StringComparison.OrdinalIgnoreCase))
                {
                    baseName = baseName.Substring(0, baseName.Length - item.Length);
                    break;
                }
            }

            if (fileToPurge.Directory == null)
            {
                LogTools.LogWarning("Unable to purge old index files; cannot determine the parent directory of " + fileToPurge.FullName);
                return 0;
            }

            // Delete all files associated with this FASTA file
            var filesToDelete = new List<FileInfo>();
            filesToDelete.AddRange(fileToPurge.Directory.GetFiles(baseName + ".*"));

            if (debugLevel >= 1)
            {
                var fileText = string.Format("{0,2} file", filesToDelete.Count);

                if (filesToDelete.Count != 1)
                {
                    fileText += "s";
                }
                LogTools.LogMessage("Deleting " + fileText + " associated with " + fileToPurge.FullName);
            }

            long bytesDeleted = 0;

            try
            {
                foreach (var fileToDelete in filesToDelete)
                {
                    var fileSizeBytes = fileToDelete.Length;

                    if (preview)
                        ConsoleMsgUtils.ShowDebugCustom("Preview delete " + fileToDelete.Name, "    ");
                    else if (fileToDelete.Exists)
                        fileToDelete.Delete();

                    bytesDeleted += fileSizeBytes;
                }
            }
            catch (Exception ex)
            {
                LogTools.LogError("Error in PurgeFastaFiles", ex);
            }

            return bytesDeleted;
        }

        /// <summary>
        /// Purge old FASTA files and related index files in the orgDb directory
        /// </summary>
        /// <remarks>
        /// This method works best on local drives (including on Linux)
        /// It will also work on a remote Windows share if the directory has file MaxDirSize.txt
        /// If file MaxDirSize.txt does exist, freeSpaceThresholdPercent, requiredFreeSpaceMB, and maxDirectorySizeGB are ignored
        /// </remarks>
        /// <param name="orgDbDirectoryPath">Organism database directory with FASTA files and related index files; supports Windows shares and Linux paths</param>
        /// <param name="freeSpaceThresholdPercent">Value between 1 and 50</param>
        /// <param name="requiredFreeSpaceMB">If greater than 0, the free space that we anticipate will be needed for the given FASTA file (in megabytes)</param>
        /// <param name="maxDirectorySizeGB">If greater than 0, the maximum amount of space that files can occupy (in gigabytes)</param>
        /// <param name="legacyFastaFileBaseName">
        /// Legacy FASTA file name (without .fasta)
        /// For split FASTA jobs, should not include the split count and segment number, e.g. should not include _25x_07 or _25x_08
        /// </param>
        /// <param name="preview">When true, preview the files that would be deleted</param>
        protected void PurgeFastaFilesIfLowFreeSpace(
            string orgDbDirectoryPath,
            int freeSpaceThresholdPercent,
            double requiredFreeSpaceMB,
            int maxDirectorySizeGB,
            string legacyFastaFileBaseName,
            bool preview = false)
        {
            if (freeSpaceThresholdPercent < 1)
                freeSpaceThresholdPercent = 1;

            if (freeSpaceThresholdPercent > 50)
                freeSpaceThresholdPercent = 50;

            try
            {
                var orgDbDirectory = new DirectoryInfo(orgDbDirectoryPath);

                // Assure that this method wasn't called on C:\ (or similar)
                if (orgDbDirectory.FullName.Length < 4)
                {
                    LogMessage("Warning: Org DB directory length is less than 4 characters; this is unexpected: " + orgDbDirectory.FullName);
                    return;
                }

                // Look for file MaxDirSize.txt which defines the maximum space that the files can use
                var maxDirSizeFile = new FileInfo(Path.Combine(orgDbDirectory.FullName, "MaxDirSize.txt"));

                if (maxDirSizeFile.Exists)
                {
                    // MaxDirSize.txt file exists; this file specifies the max total GB that files in orgDbDirectory can use
                    // If the file exists and has a valid threshold, we will not delete files using freeSpaceThresholdPercent, requiredFreeSpaceMB, or maxDirectorySizeGB
                    var success = PurgeFastaFilesUsingSpaceUsedThreshold(maxDirSizeFile, legacyFastaFileBaseName, mDebugLevel, preview);

                    if (success)
                        return;
                }

                if (maxDirectorySizeGB > 0)
                {
                    // Assure that the total size of files in the directory is below a threshold
                    PurgeFastaFilesUsingSpaceUsedThreshold(orgDbDirectory, maxDirectorySizeGB, legacyFastaFileBaseName, mDebugLevel, preview);
                }

                var localDriveInfo = mDirectorySpaceTools.GetLocalDriveInfo(orgDbDirectory);

                if (localDriveInfo == null)
                {
                    // Could not instantiate the DriveInfo class (and MaxDirSize.txt does not exist)

                    string baseErrorMessage;

                    if (Path.DirectorySeparatorChar == '/')
                        baseErrorMessage = "Could not determine the root path, and could not find file " + maxDirSizeFile.Name;
                    else
                        baseErrorMessage = "Orb DB directory path does not have a colon and could not find file " + maxDirSizeFile.Name;

                    LogWarning("Warning: {0}; cannot manage drive space usage: {1}", baseErrorMessage, orgDbDirectory.FullName);

                    LogMessage(
                        "Create file {0} with 'MaxSizeGB=50' on a single line. " +
                        "Comment lines are allowed using # as a comment character", maxDirSizeFile.Name);

                    return;
                }

                var percentFreeSpaceAtStart = localDriveInfo.AvailableFreeSpace / (double)localDriveInfo.TotalSize * 100;

                if (percentFreeSpaceAtStart >= freeSpaceThresholdPercent)
                {
                    if (mDebugLevel >= 2)
                    {
                        var freeSpaceGB = Global.BytesToGB(localDriveInfo.AvailableFreeSpace);
                        LogMessage("Free space on {0} ({1:F1} GB) is over {2}% of the total space; purge not required", localDriveInfo.Name, freeSpaceGB, freeSpaceThresholdPercent);
                    }
                }
                else
                {
                    PurgeFastaFilesUsingFreeSpaceThreshold(
                        localDriveInfo, orgDbDirectory, legacyFastaFileBaseName,
                        freeSpaceThresholdPercent, requiredFreeSpaceMB, percentFreeSpaceAtStart, preview);
                }
            }
            catch (Exception ex)
            {
                LogError("Error in PurgeFastaFilesIfLowFreeSpace", ex);
            }
        }

        /// <summary>
        /// Purge FASTA Files until the drive free space falls below a threshold
        /// This method is Windows specific
        /// </summary>
        /// <param name="localDriveInfo">Local drive info</param>
        /// <param name="orgDbDirectory">Organism DB directory</param>
        /// <param name="legacyFastaFileBaseName">Legacy FASTA file base name</param>
        /// <param name="freeSpaceThresholdPercent">Target free space threshold</param>
        /// <param name="requiredFreeSpaceMB">Required free space, in MB</param>
        /// <param name="percentFreeSpaceAtStart">Percent free space before deleting files</param>
        /// <param name="preview">If true, preview deletes</param>
        private void PurgeFastaFilesUsingFreeSpaceThreshold(
            DriveInfo localDriveInfo,
            DirectoryInfo orgDbDirectory,
            string legacyFastaFileBaseName,
            int freeSpaceThresholdPercent,
            double requiredFreeSpaceMB,
            double percentFreeSpaceAtStart,
            bool preview)
        {
            var freeSpaceGB = Global.BytesToGB(localDriveInfo.AvailableFreeSpace);

            var logInfoMessages =
                mDebugLevel >= 1 && freeSpaceGB < 100 ||
                mDebugLevel >= 2 && freeSpaceGB < 250 ||
                mDebugLevel >= 3;

            if (logInfoMessages)
            {
                LogMessage("Free space on {0} ({1:F1} GB) is {2:F1}% of the total space; purge required since less than threshold of {3}%",
                    localDriveInfo.Name, freeSpaceGB, percentFreeSpaceAtStart, freeSpaceThresholdPercent);
            }

            // Obtain a dictionary of FASTA files where Keys are FileInfo and values are last usage date
            var fastaFiles = GetFastaFilesByLastUse(orgDbDirectory);

            var fastaFilesByLastUse = (from item in fastaFiles orderby item.Value select item.Key);
            long totalBytesPurged = 0;

            foreach (var fileToPurge in fastaFilesByLastUse)
            {
                // Abort this process if the LastUsed date of this file is less than 5 days old
                if (fastaFiles.TryGetValue(fileToPurge, out var lastUsed))
                {
                    if (DateTime.UtcNow.Subtract(lastUsed).TotalDays < 5)
                    {
                        if (logInfoMessages)
                        {
                            LogMessage("All FASTA files in " + orgDbDirectory.FullName + " are less than 5 days old; " +
                                "will not purge any more files to free disk space");
                        }
                        break;
                    }
                }

                // Delete all files associated with this FASTA file
                // However, do not delete it if the name starts with legacyFastaFileBaseName
                var bytesDeleted = PurgeFastaFiles(fileToPurge, legacyFastaFileBaseName, mDebugLevel, preview);
                totalBytesPurged += bytesDeleted;

                // Re-check the disk free space
                double updatedFreeSpaceGB;
                double percentFreeSpace;

                if (preview)
                {
                    updatedFreeSpaceGB = freeSpaceGB + Global.BytesToGB(totalBytesPurged);
                    percentFreeSpace = updatedFreeSpaceGB * 1024 * 1024 * 1024 / localDriveInfo.TotalSize * 100;
                }
                else
                {
                    updatedFreeSpaceGB = Global.BytesToGB(localDriveInfo.AvailableFreeSpace);
                    percentFreeSpace = localDriveInfo.AvailableFreeSpace / (double)localDriveInfo.TotalSize * 100;
                }

                if (requiredFreeSpaceMB > 0 && updatedFreeSpaceGB * 1024.0 < requiredFreeSpaceMB)
                {
                    // Required free space is known, and we're not yet there
                    // Keep deleting files
                    if (mDebugLevel >= 2)
                    {
                        LogDebugMessage(string.Format("Free space on {0} ({1:F1} GB) is now {2:F1}% of the total space", localDriveInfo.Name, updatedFreeSpaceGB, percentFreeSpace));
                    }
                }
                else
                {
                    // Either required free space is not known, or we have more than enough free space

                    if (percentFreeSpace >= freeSpaceThresholdPercent)
                    {
                        // Target threshold reached
                        if (mDebugLevel >= 1)
                        {
                            LogMessage("Free space on {0} ({1:F1} GB) is now over {2}% of the total space; deleted {3:F1} GB of cached files",
                                localDriveInfo.Name, updatedFreeSpaceGB, freeSpaceThresholdPercent, Global.BytesToGB(totalBytesPurged));
                        }
                        break;
                    }

                    if (mDebugLevel >= 2)
                    {
                        // Keep deleting until we reach the target threshold for free space
                        LogDebugMessage(string.Format("Free space on {0} ({1:F1} GB) is now {2:F1}% of the total space", localDriveInfo.Name, updatedFreeSpaceGB, percentFreeSpace));
                    }
                }
            }

            // We have deleted every file that can be deleted

            double finalFreeSpaceGB;

            if (preview)
            {
                finalFreeSpaceGB = freeSpaceGB + Global.BytesToGB(totalBytesPurged);
            }
            else
            {
                finalFreeSpaceGB = Global.BytesToGB(localDriveInfo.AvailableFreeSpace);
            }

            if (requiredFreeSpaceMB > 0 && finalFreeSpaceGB * 1024.0 < requiredFreeSpaceMB)
            {
                LogMessage(
                    "Warning: unable to delete enough files to free up the required space on {0} " +
                    "({1:F1} GB vs. {2:F1} GB); deleted {3:F1} GB of cached files",
                    localDriveInfo.Name, finalFreeSpaceGB, requiredFreeSpaceMB / 1024.0, Global.BytesToGB(totalBytesPurged));
            }
        }

        /// <summary>
        /// Use the space usage threshold defined in MaxDirSize.txt to decide if any FASTA files need to be deleted
        /// </summary>
        /// <param name="maxDirSizeFile">MaxDirSize.txt file in the organism DB directory (or parent directory if processing files on a remote share, e.g. \\gigasax\MSGFPlus_Index_Files)</param>
        /// <param name="legacyFastaFileBaseName">Base FASTA file name for the current analysis job</param>
        /// <param name="debugLevel">Debug Level for logging; 1=minimal logging; 5=detailed logging</param>
        /// <param name="preview">When true, preview the files that would be deleted</param>
        /// <returns>True if the MaxDirSize.txt file exists and has a valid MaxSizeGB threshold</returns>
        public static bool PurgeFastaFilesUsingSpaceUsedThreshold(FileInfo maxDirSizeFile, string legacyFastaFileBaseName, short debugLevel, bool preview)
        {
            try
            {
                var orgDbDirectory = maxDirSizeFile.Directory;

                if (orgDbDirectory == null)
                {
                    LogTools.LogError("Unable to determine the parent directory of file {0}; cannot manage drive space usage", maxDirSizeFile.FullName);

                    return false;
                }

                var errorSuffix = "; cannot manage drive space usage: " + orgDbDirectory.FullName;
                var maxSizeGB = 0;

                using (var reader = new StreamReader(new FileStream(maxDirSizeFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine) || dataLine.StartsWith("#"))
                        {
                            continue;
                        }

                        var lineParts = dataLine.Split('=');

                        if (lineParts.Length < 2)
                        {
                            continue;
                        }

                        if (!string.Equals(lineParts[0], "MaxSizeGB", StringComparison.OrdinalIgnoreCase))
                            continue;

                        if (!int.TryParse(lineParts[1], out maxSizeGB))
                        {
                            LogTools.LogError("MaxSizeGB line does not contain an integer in " + maxDirSizeFile.FullName + errorSuffix);
                            return false;
                        }

                        break;
                    }
                }

                if (maxSizeGB > 0)
                {
                    return PurgeFastaFilesUsingSpaceUsedThreshold(orgDbDirectory, maxSizeGB, legacyFastaFileBaseName, debugLevel, preview);
                }

                LogTools.LogError("MaxSizeGB line not found in " + maxDirSizeFile.FullName + errorSuffix);
                return false;
            }
            catch (Exception ex)
            {
                LogTools.LogError("Error in PurgeFastaFilesUsingSpaceUsedThreshold", ex);
                return false;
            }
        }

        /// <summary>
        /// Use the space usage threshold defined in MaxDirSize.txt to decide if any FASTA files need to be deleted
        /// </summary>
        /// <remarks>Minimum allowed value for maxDirectorySizeGB is 10; will exit the method if 0</remarks>
        /// <param name="orgDbDirectory">Local FASTA file storage directory</param>
        /// <param name="maxDirectorySizeGB">The maximum amount of space that files can occupy (in gigabytes)</param>
        /// <param name="legacyFastaFileBaseName">Base FASTA file name for the current analysis job</param>
        /// <param name="debugLevel">Debug Level for logging; 1=minimal logging; 5=detailed logging</param>
        /// <param name="preview">When true, preview the files that would be deleted</param>
        /// <returns>True if maxDirectorySizeGB is positive, otherwise false</returns>
        public static bool PurgeFastaFilesUsingSpaceUsedThreshold(
            DirectoryInfo orgDbDirectory,
            int maxDirectorySizeGB,
            string legacyFastaFileBaseName,
            short debugLevel,
            bool preview)
        {
            const int MIN_DIRECTORY_SIZE_GB = 10;

            try
            {
                // ReSharper disable once ConvertIfStatementToSwitchStatement
                if (maxDirectorySizeGB <= 0)
                {
                    LogTools.LogWarning("PurgeFastaFilesUsingSpaceUsedThreshold should be called with a positive integer, not {0}; aborting", maxDirectorySizeGB);

                    return false;
                }

                if (maxDirectorySizeGB < MIN_DIRECTORY_SIZE_GB)
                {
                    LogTools.LogWarning("Max directory size sent to PurgeFastaFilesUsingSpaceUsedThreshold is too small; increasing from {0} GB to {1} GB", maxDirectorySizeGB, MIN_DIRECTORY_SIZE_GB);

                    maxDirectorySizeGB = MIN_DIRECTORY_SIZE_GB;
                }

                var purgeSuccessful = false;
                long totalBytesPurgedOverall = 0;

                // This variable is a safety measure to assure the while loop doesn't continue indefinitely
                // Since we're deleting data in chunks of 10 GB and max iterations is 100,
                // this method will purge, at most, 1000 GB of old FASTA files
                var iterations = 0;

                while (iterations < 100)
                {
                    iterations++;

                    long spaceUsageBytes = 0;

                    foreach (var orgDbDirFile in orgDbDirectory.GetFiles("*", SearchOption.AllDirectories))
                    {
                        spaceUsageBytes += orgDbDirFile.Length;
                    }

                    var spaceUsageGB = Global.BytesToGB(spaceUsageBytes);

                    if (spaceUsageGB <= maxDirectorySizeGB)
                    {
                        // Space usage is under the threshold
                        var statusMessage = string.Format(
                            "Space usage in {0} is {1:F1} GB, which is below the threshold of {2} GB; nothing to purge",
                            orgDbDirectory.FullName, spaceUsageGB, maxDirectorySizeGB);

                        if (debugLevel >= 3)
                        {
                            LogTools.LogDebug(statusMessage);
                        }
                        else
                        {
                            ConsoleMsgUtils.ShowDebug(statusMessage);
                        }

                        return true;
                    }

                    // Space usage is too high; need to purge some files
                    // Obtain a dictionary of FASTA files where Keys are FileInfo and values are last usage date
                    var fastaFiles = GetFastaFilesByLastUse(orgDbDirectory);

                    if (fastaFiles.Count == 0)
                    {
                        LogTools.LogWarning("Did not find any FASTA files to purge in " + orgDbDirectory.FullName);

                        // Even though we found no FASTA files to purge, return true since MaxDirSize.txt was found
                        return true;
                    }

                    var fastaFilesByLastUse = (from item in fastaFiles orderby item.Value select item.Key).ToList();

                    var bytesToPurge = (long)(spaceUsageBytes - maxDirectorySizeGB * 1024.0 * 1024 * 1024);
                    long totalBytesPurged = 0;

                    var filesProcessed = 0;

                    foreach (var fileToPurge in fastaFilesByLastUse)
                    {
                        filesProcessed++;

                        // Abort this process if the LastUsed date of this file is less than 5 days old
                        if (fastaFiles.TryGetValue(fileToPurge, out var lastUsed) &&
                            DateTime.UtcNow.Subtract(lastUsed).TotalDays < 5)
                        {
                            LogTools.LogMessage("All FASTA files in " + orgDbDirectory.FullName + " are less than 5 days old; " +
                                                "will not purge any more files to free disk space");

                            filesProcessed = fastaFilesByLastUse.Count;
                            break;
                        }

                        // Delete all files associated with this FASTA file
                        // However, do not delete it if the name starts with legacyFastaFileBaseName
                        var bytesDeleted = PurgeFastaFiles(fileToPurge, legacyFastaFileBaseName, debugLevel, preview);
                        totalBytesPurged += bytesDeleted;
                        totalBytesPurgedOverall += bytesDeleted;

                        if (totalBytesPurged < bytesToPurge)
                        {
                            // Keep deleting files
                            if (debugLevel >= 2)
                            {
                                LogTools.LogDebug("Purging FASTA files: {0:F1} / {1:F1} MB deleted", Global.BytesToMB(totalBytesPurged), Global.BytesToMB(bytesToPurge));
                            }

                            if (Global.BytesToGB(totalBytesPurged) > 10)
                            {
                                // We have deleted 10 GB of data; re-scan for FASTA files to delete
                                // This is done in case two managers are actively purging files from the same directory simultaneously
                                break;
                            }
                        }
                        else
                        {
                            // Enough files have been deleted
                            LogTools.LogMessage("Space usage in {0} is now below {1} GB; deleted {2:F1} GB of cached files", orgDbDirectory.FullName, maxDirectorySizeGB, Global.BytesToGB(totalBytesPurgedOverall));

                            return true;
                        }
                    }

                    if (filesProcessed < fastaFilesByLastUse.Count)
                        continue;

                    purgeSuccessful = totalBytesPurged >= bytesToPurge;
                    break;
                } // end while

                if (!purgeSuccessful)
                {
                    LogTools.LogWarning("Warning: unable to delete enough files to lower the space usage in {0} to below {1} GB; " +
                                        "deleted {2:F1} GB of cached files", orgDbDirectory.FullName, maxDirectorySizeGB, Global.BytesToGB(totalBytesPurgedOverall));
                }

                return true;
            }
            catch (Exception ex)
            {
                LogTools.LogError("Error in PurgeFastaFilesUsingSpaceUsedThreshold", ex);
                return false;
            }
        }

        private bool RemoteFastaFilesMatch(
            FileInfo sourceFasta, FileSystemInfo sourceHashcheck,
            SftpFile remoteFasta, ISftpFile remoteHashcheck,
            RemoteTransferUtility transferUtility)
        {
            var remoteHostName = transferUtility.RemoteHostName;

            if (remoteFasta == null)
            {
                LogDebug("FASTA file not found on remote host; copying {0} to {1}", sourceFasta.Name, remoteHostName);
                return false;
            }

            if (remoteHashcheck == null)
            {
                LogDebug("FASTA .hashcheck file not found on remote host; copying {0} to {1}", sourceFasta.Name, remoteHostName);
                return false;
            }

            // Compare FASTA file lengths plus the names of the HashCheck files
            // Do not compare the contents (or even the length) of the HashCheck files since the source HashCheck file gets updated every few days
            if (remoteFasta.Length == sourceFasta.Length && remoteHashcheck.Name == sourceHashcheck.Name)
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
                    LogError(string.Format(
                        "File size mismatch; WaitForRemoteFileCopy reports abortCopy=true for remote file {0}",
                        remoteFasta.FullName));
                    return false;
                }

                if (filesMatch)
                {
                    LogDebug("Using existing FASTA file {0} on {1}", remoteFasta.FullName, transferUtility.RemoteHostName);
                    return true;
                }

                LogDebug("Copying {0} to {1}", sourceFasta.Name, transferUtility.RemoteHostName);
            }
            else
            {
                LogDebug("FASTA file size on remote host is different than local file ({0} bytes vs. {1} bytes locally); " +
                         "copying {2} to {3}", remoteFasta.Length, sourceFasta.Length, sourceFasta.Name, transferUtility.RemoteHostName);
            }

            return false;
        }

        /// <summary>
        /// Looks for the specified file in the given directory
        /// If present, returns the full path to the file
        /// If not present, looks for a file named FileName_StoragePathInfo.txt; if that file is found, opens the file and reads the path
        /// If the file isn't found (and the _StoragePathInfo.txt file isn't present), returns an empty string
        /// </summary>
        /// <param name="directoryPath">the directory to look in</param>
        /// <param name="fileName">The file name to find</param>
        public static string ResolveStoragePath(string directoryPath, string fileName)
        {
            var filePath = Path.Combine(directoryPath, fileName);

            if (File.Exists(filePath))
            {
                // The desired file is located in directoryPath
                return filePath;
            }

            // The desired file was not found
            var storagePathInfoFilePath = filePath + STORAGE_PATH_INFO_FILE_SUFFIX;

            if (!File.Exists(storagePathInfoFilePath))
                return string.Empty;

            // The _StoragePathInfo.txt file is present
            // Open that file to read the file path on the first line of the file

            using var reader = new StreamReader(new FileStream(storagePathInfoFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

            if (reader.EndOfStream)
                return string.Empty;

            var physicalFilePath = reader.ReadLine();

            return physicalFilePath;
        }

        /// <summary>
        /// Looks for the STORAGE_PATH_INFO_FILE_SUFFIX file in the working directory
        /// If present, looks for a file named _StoragePathInfo.txt; if that file is found, opens the file and reads the path
        /// If the file named _StoragePathInfo.txt isn't found, looks for a ser file in the specified directory
        /// If found, returns the path to the ser file
        /// If not found, looks for a 0.ser folder in the specified folder
        /// If found, returns the path to the 0.ser folder
        /// Otherwise, returns an empty string
        /// </summary>
        /// <param name="folderPath">the directory to look in</param>
        public static string ResolveSerStoragePath(string folderPath)
        {
            var filePath = Path.Combine(folderPath, STORAGE_PATH_INFO_FILE_SUFFIX);

            if (File.Exists(filePath))
            {
                // The desired file is located in directory FolderPath
                // The _StoragePathInfo.txt file is present
                // Open that file to read the file path on the first line of the file

                using var reader = new StreamReader(new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                var physicalFilePath = reader.ReadLine();
                return physicalFilePath;
            }

            // The desired file was not found

            // Look for a ser file in the dataset directory
            var serFilePath = Path.Combine(folderPath, BRUKER_SER_FILE);
            var serFile = new FileInfo(serFilePath);

            if (serFile.Exists)
                return serFilePath;

            // See if a folder named 0.ser exists in FolderPath
            var zeroSerDirectoryPath = Path.Combine(folderPath, BRUKER_ZERO_SER_FOLDER);
            var zeroSerDirectory = new DirectoryInfo(zeroSerDirectoryPath);

            if (zeroSerDirectory.Exists)
                return zeroSerDirectoryPath;

            return string.Empty;
        }

        // Ignore Spelling: nocopy

        /// <summary>
        /// Retrieve the files specified by the file processing options parameter
        /// </summary>
        /// <remarks>
        /// This method is used by plugins PhosphoFDRAggregator and PRIDEMzXML
        /// However, PrideMzXML is dormant as of September 2013
        /// </remarks>
        /// <param name="fileSpecList">
        /// File processing options, examples:
        /// sequest:_syn.txt:nocopy,sequest:_fht.txt:nocopy,sequest:_dta.zip:nocopy,sequest:_syn_ModSummary.txt:nocopy,masic_finnigan:_ScanStatsEx.txt:nocopy
        /// sequest:_syn.txt,sequest:_syn_MSGF.txt,sequest:_fht.txt,sequest:_fht_MSGF.txt,sequest:_dta.zip,sequest:_syn_ModSummary.txt
        /// MSGFPlus:_msgfplus_syn.txt,MSGFPlus:_msgfplus_fht.txt,MSGFPlus:_dta.zip,MSGFPlus:_syn_ModSummary.txt,masic_finnigan:_ScanStatsEx.txt,masic_finnigan:_ReporterIons.txt:copy
        /// MSGFPlus:_msgfplus_syn.txt,MSGFPlus:_msgfplus_syn_ModSummary.txt,MSGFPlus:_dta.zip
        /// </param>
        /// <param name="fileRetrievalMode">Used by plugins to indicate the types of files that are required (in case fileSpecList is not configured correctly for a given data package job)</param>
        /// <param name="callingMethodCanRegenerateMissingFile">True if the calling method has logic defined for generating the .mzML file if it is not found</param>
        /// <param name="dataPackageJobs">Output: dictionary of data package jobs; keys are job numbers, values are data package info instances</param>
        /// <returns>True if success, false if a problem</returns>
        protected bool RetrieveAggregateFiles(
            List<string> fileSpecList,
            DataPackageFileRetrievalModeConstants fileRetrievalMode,
            bool callingMethodCanRegenerateMissingFile,
            out Dictionary<int, DataPackageJobInfo> dataPackageJobs)
        {
            bool success;

            try
            {
                // Note that LoadDataPackageJobInfo does not update NumberOfClonedSteps in dataPackageJobs
                // RetrieveAggregateFiles does not support Split-FASTA jobs, so it does not need NumberOfClonedSteps and thus no need to call RetrieveDataPackagePeptideHitJobInfo
                if (!LoadDataPackageJobInfo(out dataPackageJobs))
                {
                    UpdateStatusMessage("Error looking up datasets and jobs using LoadDataPackageJobInfo");
                    dataPackageJobs = null;
                    return false;
                }
            }
            catch (Exception ex)
            {
                LogError("RetrieveAggregateFiles; Exception calling LoadDataPackageJobInfo", ex);
                dataPackageJobs = null;
                return false;
            }

            try
            {
                var workingDirectory = new DirectoryInfo(mWorkDir);

                // Cache the current dataset and job info
                var currentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo();

                foreach (var dataPkgJob in dataPackageJobs)
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

                        if (dataPkgJob.Value.Tool.StartsWith(fileSpecTerms[0].Trim(), StringComparison.OrdinalIgnoreCase))
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

                                if (dataPkgJob.Value.Tool.StartsWith("msgf", StringComparison.OrdinalIgnoreCase))
                                {
                                    // MS-GF+
                                    fileSpecListCurrent = new List<string> {
                                        "MSGFPlus:_msgfplus_syn.txt",
                                        "MSGFPlus:_msgfplus_syn_ModSummary.txt",
                                        "MSGFPlus:_dta.zip"
                                    };
                                }

                                if (dataPkgJob.Value.Tool.StartsWith("sequest", StringComparison.OrdinalIgnoreCase))
                                {
                                    // Sequest
                                    fileSpecListCurrent = new List<string> {
                                        "sequest:_syn.txt",
                                        "sequest:_syn_MSGF.txt",
                                        "sequest:_syn_ModSummary.txt",
                                        "sequest:_dta.zip"
                                    };
                                }

                                if (dataPkgJob.Value.Tool.StartsWith("xtandem", StringComparison.OrdinalIgnoreCase))
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
                        var sourceDirectoryPath = "??";

                        var saveMode = "nocopy";

                        if (fileSpecTerms.Count > 2)
                        {
                            saveMode = fileSpecTerms[2].Trim();
                        }

                        try
                        {
                            if (!dataPkgJob.Value.Tool.StartsWith(fileSpecTerms[0].Trim(), StringComparison.OrdinalIgnoreCase))
                            {
                                continue;
                            }

                            // To avoid collisions, files for this job will be placed in a subdirectory based on the Job number
                            var targetDirectory = new DirectoryInfo(Path.Combine(mWorkDir, "Job" + dataPkgJob.Key));

                            if (!targetDirectory.Exists)
                                targetDirectory.Create();

                            if (sourceFileName.EndsWith(CDTA_ZIPPED_EXTENSION, StringComparison.OrdinalIgnoreCase) &&
                                dataPkgJob.Value.Tool.EndsWith("_mzml", StringComparison.OrdinalIgnoreCase))
                            {
                                // This is a .mzML job; it is not going to have a _dta.zip file
                                // Setting sourceDirectoryPath to an empty string so that GetMzMLFile will get called below
                                sourceDirectoryPath = string.Empty;
                            }
                            else
                            {
                                sourceDirectoryPath = FileSearchTool.FindDataFile(sourceFileName);

                                if (string.IsNullOrEmpty(sourceDirectoryPath))
                                {
                                    // Source file not found

                                    var alternateSourceFileName = string.Empty;

                                    if (sourceFileName.IndexOf("_msgfdb", StringComparison.OrdinalIgnoreCase) >= 0)
                                    {
                                        // Auto-look for the _msgfplus version of this file
                                        alternateSourceFileName = Global.ReplaceIgnoreCase(sourceFileName, "_msgfdb", "_msgfplus");
                                    }
                                    else if (sourceFileName.IndexOf("_msgfplus", StringComparison.OrdinalIgnoreCase) >= 0)
                                    {
                                        // Auto-look for the _msgfdb version of this file
                                        alternateSourceFileName = Global.ReplaceIgnoreCase(sourceFileName, "_msgfplus", "_msgfdb");
                                    }

                                    if (!string.IsNullOrEmpty(alternateSourceFileName))
                                    {
                                        sourceDirectoryPath = FileSearchTool.FindDataFile(alternateSourceFileName);

                                        if (!string.IsNullOrEmpty(sourceDirectoryPath))
                                        {
                                            sourceFileName = alternateSourceFileName;
                                        }
                                    }
                                }
                            }

                            if (string.IsNullOrEmpty(sourceDirectoryPath))
                            {
                                if (sourceFileName.EndsWith(CDTA_ZIPPED_EXTENSION, StringComparison.OrdinalIgnoreCase))
                                {
                                    // Look for a mzML.gz file instead

                                    var retrieved = FileSearchTool.RetrieveCachedMSXMLFile(
                                        DOT_MZML_EXTENSION, false, callingMethodCanRegenerateMissingFile, false, true,
                                        out var errorMessage, out _, out _);

                                    if (!retrieved)
                                    {
                                        // No _dta.zip and no mzML.gz file; abort processing
                                        if (string.IsNullOrWhiteSpace(errorMessage))
                                        {
                                            errorMessage = "Unknown error looking for the .mzML file for " + dataPkgJob.Value.Dataset + ", job " + dataPkgJob.Key;
                                        }

                                        LogError(errorMessage);
                                        continue;
                                    }

                                    sourceFileName = dataPkgJob.Value.Dataset + DOT_MZML_EXTENSION + DOT_GZ_EXTENSION;
                                    mJobParams.AddAdditionalParameter("DataPackageMetadata", spectraFileKey, sourceFileName);
                                    mJobParams.AddResultFileExtensionToSkip(DOT_GZ_EXTENSION);

                                    MoveFileToFolder(workingDirectory, targetDirectory, sourceFileName);

                                    if (mDebugLevel >= 1)
                                    {
                                        LogMessage("Retrieved the .mzML file for {0}, job {1}, from {2}", dataPkgJob.Value.Dataset, dataPkgJob.Key, sourceDirectoryPath);
                                    }

                                    continue;
                                }

                                UpdateStatusMessage("Could not find a valid directory with file " + sourceFileName + " for job " + dataPkgJob.Key);

                                if (mDebugLevel >= 1)
                                {
                                    LogMessage(mMessage, 0, true);
                                }
                                return false;
                            }

                            if (!mFileCopyUtilities.CopyFileToWorkDir(sourceFileName, sourceDirectoryPath, mWorkDir, BaseLogger.LogLevels.ERROR))
                            {
                                UpdateStatusMessage("CopyFileToWorkDir returned false for " + sourceFileName + " using directory " + sourceDirectoryPath + " for job " + dataPkgJob.Key);

                                if (mDebugLevel >= 1)
                                {
                                    LogMessage(mMessage, 0, true);
                                }
                                return false;
                            }

                            if (sourceFileName.EndsWith(CDTA_ZIPPED_EXTENSION, StringComparison.OrdinalIgnoreCase))
                            {
                                mJobParams.AddAdditionalParameter("DataPackageMetadata", spectraFileKey, sourceFileName);
                            }

                            if (!string.Equals(saveMode, "copy", StringComparison.OrdinalIgnoreCase))
                            {
                                mJobParams.AddResultFileToSkip(sourceFileName);
                            }

                            MoveFileToFolder(workingDirectory, targetDirectory, sourceFileName);

                            if (mDebugLevel >= 1)
                            {
                                LogMessage("Copied " + sourceFileName + " from directory " + sourceDirectoryPath);
                            }
                        }
                        catch (Exception ex)
                        {
                            LogError("RetrieveAggregateFiles; Exception during copy of file: " + sourceFileName + " from directory " + sourceDirectoryPath + " for job " + dataPkgJob.Key, ex);
                            return false;
                        }
                    }
                }

                // ReSharper disable once RedundantNameQualifier
                if (!mMyEMSLUtilities.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
                {
                    return false;
                }

                // Restore the dataset and job info for this aggregation job
                OverrideCurrentDatasetAndJobInfo(currentDatasetAndJobInfo);

                success = true;
            }
            catch (Exception ex)
            {
                LogError("Error in RetrieveAggregateFiles", ex);
                success = false;
            }

            return success;
        }

        /// <summary>
        /// Retrieves the PHRP files for the PeptideHit jobs defined for the data package associated with this aggregation job
        /// Also creates a batch file that can be manually run to retrieve the instrument data files
        /// </summary>
        /// <param name="retrievalOptions">File retrieval options</param>
        /// <param name="dataPackagePeptideHitJobs">Output: Job info for the peptide_hit jobs associated with this data package</param>
        /// <returns>True if success, false if an error</returns>
        protected bool RetrieveDataPackagePeptideHitJobPHRPFiles(
            DataPackageFileHandler.DataPackageRetrievalOptionsType retrievalOptions,
            out List<DataPackageJobInfo> dataPackagePeptideHitJobs)
        {
            const float progressPercentAtStart = 0;
            const float progressPercentAtFinish = 20;
            return RetrieveDataPackagePeptideHitJobPHRPFiles(retrievalOptions, out dataPackagePeptideHitJobs, progressPercentAtStart, progressPercentAtFinish);
        }

        /// <summary>
        /// Retrieves the PHRP files for the PeptideHit jobs defined for the data package associated with this aggregation job
        /// Also creates a batch file that can be manually run to retrieve the instrument data files
        /// </summary>
        /// <param name="retrievalOptions">File retrieval options</param>
        /// <param name="dataPackagePeptideHitJobs">Output: Job info for the peptide_hit jobs associated with this data package</param>
        /// <param name="progressPercentAtStart">Percent complete value to use for computing incremental progress</param>
        /// <param name="progressPercentAtFinish">Percent complete value to use for computing incremental progress</param>
        /// <returns>True if success, false if an error</returns>
        protected bool RetrieveDataPackagePeptideHitJobPHRPFiles(
            DataPackageFileHandler.DataPackageRetrievalOptionsType retrievalOptions,
            out List<DataPackageJobInfo> dataPackagePeptideHitJobs,
            float progressPercentAtStart,
            float progressPercentAtFinish)
        {
            // SQL Server: Data Source=Gigasax;Initial Catalog=DMS_Pipeline
            // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms
            var brokerDbConnectionString = mMgrParams.GetParam("BrokerConnectionString");

            var dataPackageID = mJobParams.GetJobParameter("DataPackageID", -1);

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(brokerDbConnectionString, mMgrName);

            var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: TraceMode);
            RegisterEvents(dbTools);

            var dataPackageFileHandler = new DataPackageFileHandler(dbTools, dataPackageID, this);
            RegisterEvents(dataPackageFileHandler);

            var success = dataPackageFileHandler.RetrieveDataPackagePeptideHitJobPHRPFiles(
                retrievalOptions, out dataPackagePeptideHitJobs,
                progressPercentAtStart, progressPercentAtFinish);

            mJobParams.AddResultFileToSkip(DataPackageFileHandler.DATA_PKG_JOB_METADATA_FILE);

            return success;
        }

        /// <summary>
        /// Create a FASTA file for Sequest, X!Tandem, Inspect, or MSGFPlus analysis
        /// </summary>
        /// <remarks>Stores the name of the FASTA file as a new job parameter named "GeneratedFastaName" in section "PeptideSearch"</remarks>
        /// <param name="orgDbDirectoryPath">Directory on analysis machine where FASTA files are stored</param>
        /// <param name="resultCode">Output: status code</param>
        /// <param name="previewMode">Set to true to show the filename that would be retrieved</param>
        /// <returns>True if success, false if an error</returns>
        protected bool RetrieveOrgDB(string orgDbDirectoryPath, out CloseOutType resultCode, bool previewMode = false)
        {
            const int maxLegacyFASTASizeGB = 100;
            return RetrieveOrgDB(orgDbDirectoryPath, out resultCode, maxLegacyFASTASizeGB, out _, previewMode: previewMode);
        }

        /// <summary>
        /// Create a FASTA file for Sequest, X!Tandem, Inspect, or MSGFPlus analysis
        /// </summary>
        /// <remarks>Stores the name of the FASTA file as a new job parameter named "GeneratedFastaName" in section "PeptideSearch"</remarks>
        /// <param name="orgDbDirectoryPath">Directory on analysis machine where FASTA files are stored</param>
        /// <param name="resultCode">Output: status code</param>
        /// <param name="maxLegacyFASTASizeGB">
        /// Maximum FASTA file size to retrieve when retrieving a legacy (standalone) FASTA file
        /// Returns false if the file was not copied because it is too large</param>
        /// <param name="fastaFileSizeGB">Output: FASTA file size, in GB</param>
        /// <param name="decoyProteinsUseXXX">When true, decoy protein names start with XXX_ (defaults to true as of April 2019)</param>
        /// <param name="previewMode">Set to true to show the filename that would be retrieved</param>
        /// <returns>True if success, false if an error</returns>
        protected bool RetrieveOrgDB(
            string orgDbDirectoryPath,
            out CloseOutType resultCode,
            float maxLegacyFASTASizeGB,
            out double fastaFileSizeGB,
            bool decoyProteinsUseXXX = true,
            bool previewMode = false)
        {
            const int FREE_SPACE_THRESHOLD_PERCENT = 20;
            const int DEFAULT_ORG_DB_DIR_MAX_SIZE_GB = 200;

            Console.WriteLine();

            if (mDebugLevel >= 3)
            {
                LogMessage("Obtaining Org DB file");
            }

            fastaFileSizeGB = 0;

            try
            {
                var orgDBDirMaxSizeGB = mMgrParams.GetParam("OrgDBDirMaxSizeGB", DEFAULT_ORG_DB_DIR_MAX_SIZE_GB);

                var proteinCollectionInfo = new ProteinCollectionInfo(mJobParams);

                var legacyFastaFileBaseName = string.Empty;

                if (proteinCollectionInfo.UsingLegacyFasta &&
                    !string.IsNullOrWhiteSpace(proteinCollectionInfo.LegacyFastaName) &&
                    !string.Equals(proteinCollectionInfo.LegacyFastaName, "na", StringComparison.OrdinalIgnoreCase))
                {
                    legacyFastaFileBaseName = Path.GetFileNameWithoutExtension(proteinCollectionInfo.LegacyFastaName);
                }

                if (Global.OfflineMode)
                {
                    var fastaFileName = mJobParams.GetJobParameter(JOB_PARAM_GENERATED_FASTA_NAME, string.Empty);

                    if (string.IsNullOrWhiteSpace(fastaFileName))
                    {
                        LogError(string.Format("Job parameter {0} is undefined", JOB_PARAM_GENERATED_FASTA_NAME));
                        resultCode = CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                        return false;
                    }

                    // Confirm that the FASTA file exists
                    var fastaFile = new FileInfo(Path.Combine(orgDbDirectoryPath, fastaFileName));

                    if (!fastaFile.Exists)
                    {
                        LogError(string.Format("FASTA file not found: {0}", fastaFile.FullName));
                        resultCode = CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                        return false;
                    }

                    // Validate the FASTA file against the .localhashcheck file (created if missing)
                    var success = ValidateOfflineFASTA(fastaFile);

                    if (success && !previewMode)
                    {
                        UpdateLastUsedFile(fastaFile);
                    }

                    if (!previewMode)
                    {
                        PurgeFastaFilesIfLowFreeSpace(orgDbDirectoryPath, FREE_SPACE_THRESHOLD_PERCENT, 0, orgDBDirMaxSizeGB, legacyFastaFileBaseName);
                    }

                    if (success)
                        resultCode = CloseOutType.CLOSEOUT_SUCCESS;
                    else
                        resultCode = CloseOutType.CLOSEOUT_FILE_NOT_FOUND;

                    return success;
                }

                double requiredFreeSpaceMB = 0;

                if (proteinCollectionInfo.UsingLegacyFasta)
                {
                    // Estimate the drive space required to download the FASTA file and its associated MS-GF+ index files
                    requiredFreeSpaceMB = LookupLegacyDBDiskSpaceRequiredMB(proteinCollectionInfo, out var legacyFastaName, out fastaFileSizeGB);

                    if (fastaFileSizeGB > maxLegacyFASTASizeGB)
                    {
                        LogWarning(
                            "Not retrieving FASTA file {0} since it is {1:F2} GB, which is larger than the max size threshold of {2:F1} GB",
                            legacyFastaName, fastaFileSizeGB, maxLegacyFASTASizeGB);

                        resultCode = CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                        return false;
                    }
                }

                // Delete old FASTA files and suffix array files if getting low on disk space
                // Do not delete any files related to the current Legacy FASTA file (if defined)

                if (!previewMode)
                {
                    PurgeFastaFilesIfLowFreeSpace(orgDbDirectoryPath, FREE_SPACE_THRESHOLD_PERCENT, requiredFreeSpaceMB, orgDBDirMaxSizeGB, legacyFastaFileBaseName);
                }

                // Make a new FASTA file from scratch
                if (!CreateFastaFile(proteinCollectionInfo, orgDbDirectoryPath, decoyProteinsUseXXX, previewMode))
                {
                    // There was a problem. Log entries in lower-level routines provide documentation
                    resultCode = CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    return false;
                }

                if (Math.Abs(fastaFileSizeGB) < float.Epsilon && !previewMode)
                {
                    // Determine the FASTA file size (should already be known for legacy FASTA files)
                    var fastaFile = new FileInfo(Path.Combine(orgDbDirectoryPath, mFastaFileName));

                    if (fastaFile.Exists)
                        fastaFileSizeGB = Global.BytesToGB(fastaFile.Length);
                }

                // FASTA file was successfully generated.
                // Put the name of the generated FASTA file in the job data class for other methods to use
                if (!mJobParams.AddAdditionalParameter(AnalysisJob.PEPTIDE_SEARCH_SECTION, JOB_PARAM_GENERATED_FASTA_NAME, mFastaFileName))
                {
                    LogError("Error adding parameter 'GeneratedFastaName' to mJobParams");
                    resultCode = CloseOutType.CLOSEOUT_FAILED;
                    return false;
                }

                // Delete old FASTA files and suffix array files if getting low on disk space
                // No need to pass a value for legacyFastaFileBaseName because a .fasta.LastUsed file will have been created/updated by CreateFastaFile
                if (!previewMode)
                {
                    PurgeFastaFilesIfLowFreeSpace(orgDbDirectoryPath, FREE_SPACE_THRESHOLD_PERCENT, 0, 0, string.Empty);
                }

                resultCode = CloseOutType.CLOSEOUT_SUCCESS;
                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in RetrieveOrgDB", ex);
                resultCode = CloseOutType.CLOSEOUT_FAILED;
                return false;
            }
        }

        /// <summary>
        /// Uses the ParamFileGenerator DLL to obtain the parameter file defined for this job
        /// </summary>
        /// <param name="paramFileName">Name of param file to be created</param>
        /// <returns>True if success, false if an error</returns>
        protected bool RetrieveGeneratedParamFile(string paramFileName)
        {
            IGenerateFile paramFileGenerator = null;

            try
            {
                LogMessage("Retrieving parameter file " + paramFileName);

                if (Global.OfflineMode)
                {
                    var paramFile = new FileInfo(Path.Combine(mWorkDir, paramFileName));

                    if (!paramFile.Exists)
                    {
                        LogError("Parameter file not found: " + paramFile.FullName);
                        return false;
                    }

                    return true;
                }

                paramFileGenerator = new MakeParameterFile
                {
                    TemplateFilePath = mMgrParams.GetParam("ParamTemplateLoc")
                };

                // Note that job parameter "GeneratedFastaName" gets defined by RetrieveOrgDB
                // Furthermore, the full path to the FASTA file is only necessary when creating SEQUEST parameter files

                // Job parameter ToolName tracks the pipeline script name (whose name is based on the primary analysis tool for the script)
                var scriptName = mJobParams.GetParam("ToolName", string.Empty);

                if (string.IsNullOrWhiteSpace(scriptName))
                {
                    LogError("Job parameter ToolName is empty");
                    return false;
                }

                var paramFileType = SetParamFileType(scriptName);

                if (paramFileType == IGenerateFile.ParamFileType.Invalid)
                {
                    LogError("Script " + scriptName + " is not supported by the ParamFileGenerator; update AnalysisResources and ParamFileGenerator.dll");
                    return false;
                }

                var fastaFilePath = Path.Combine(mMgrParams.GetParam(MGR_PARAM_ORG_DB_DIR), mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, JOB_PARAM_GENERATED_FASTA_NAME));

                // SQL Server: Data Source=Gigasax;Initial Catalog=DMS5
                // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms
                var connectionString = mMgrParams.GetParam("ConnectionString");
                var datasetID = mJobParams.GetJobParameter(AnalysisJob.JOB_PARAMETERS_SECTION, "DatasetID", 0);

                var success = paramFileGenerator.MakeFile(paramFileName, paramFileType, fastaFilePath, mWorkDir, connectionString, datasetID);

                // Examine the size of the ModDefs.txt file
                // Add it to the ignore list if it is empty (no point in tracking a 0-byte file)
                var modDefsFile = new FileInfo(Path.Combine(mWorkDir, Path.GetFileNameWithoutExtension(paramFileName) + "_ModDefs.txt"));

                if (modDefsFile.Exists && modDefsFile.Length == 0)
                {
                    mJobParams.AddResultFileToSkip(modDefsFile.Name);
                }

                if (success)
                {
                    if (mDebugLevel >= 3)
                    {
                        LogMessage("Successfully retrieved param file: " + paramFileName);
                    }

                    return true;
                }

                if (string.IsNullOrWhiteSpace(paramFileGenerator.LastError))
                {
                    LogError("Unknown error retrieving the parameter file using the paramFileGenerator");
                }
                else
                {
                    if (string.IsNullOrWhiteSpace(mMessage))
                        LogError(paramFileGenerator.LastError);
                    else
                        LogError(mMessage + ": " + paramFileGenerator.LastError);
                }

                return false;
            }
            catch (Exception ex)
            {
                if (string.IsNullOrWhiteSpace(mMessage))
                {
                    UpdateStatusMessage("Error retrieving parameter file");
                }

                LogError(mMessage, ex);

                if (!string.IsNullOrWhiteSpace(paramFileGenerator?.LastError))
                {
                    LogMessage("Error converting param file: " + paramFileGenerator.LastError, 0, true);
                }
                return false;
            }
        }

        /// <summary>
        /// Create file JobParams.xml in the working directory using in-memory job parameters
        /// </summary>
        /// <remarks>Adds JobParams.xml to the list of files to skip by calling mJobParams.AddResultFileToSkip</remarks>
        protected void SaveCurrentJobParameters()
        {
            string xmlText;

            var memoryStream = new MemoryStream();
            using (var xWriter = new XmlTextWriter(memoryStream, Encoding.UTF8))
            {
                xWriter.Formatting = Formatting.Indented;
                xWriter.Indentation = 2;
                xWriter.IndentChar = ' ';

                // Create the XML document in memory
                xWriter.WriteStartDocument(true);

                // General job information
                // Root level element
                xWriter.WriteStartElement("sections");

                foreach (var section in mJobParams.GetAllSectionNames())
                {
                    xWriter.WriteStartElement("section");
                    xWriter.WriteAttributeString("name", section);

                    foreach (var parameter in mJobParams.GetAllParametersForSection(section))
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
                memoryStream.Seek(0, SeekOrigin.Begin);

                var memoryStreamReader = new StreamReader(memoryStream);
                xmlText = memoryStreamReader.ReadToEnd();
            }

            var jobParamsFile = new FileInfo(Path.Combine(WorkDir, AnalysisJob.OFFLINE_JOB_PARAMS_FILE));

            using var writer = new StreamWriter(new FileStream(jobParamsFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read));

            writer.WriteLine(xmlText);

            mJobParams.AddResultFileToSkip(jobParamsFile.Name);
        }

        // Ignore Spelling: Bioworks, moda, modplus, toppic

        /// <summary>
        /// Convert script name to param file type ID
        /// </summary>
        /// <param name="scriptName">Pipeline script name</param>
        /// <returns>IGenerateFile.ParamFileType based on input version</returns>
        protected IGenerateFile.ParamFileType SetParamFileType(string scriptName)
        {
            var toolNameToTypeMapping = new Dictionary<string, IGenerateFile.ParamFileType>(StringComparer.OrdinalIgnoreCase)
            {
                {"DiaNN", IGenerateFile.ParamFileType.DiaNN},
                {"DiaNN_SpecLib", IGenerateFile.ParamFileType.DiaNN},
                {"FragPipe", IGenerateFile.ParamFileType.FragPipe},
                {"Inspect", IGenerateFile.ParamFileType.Inspect},
                {"MaxQuant", IGenerateFile.ParamFileType.MaxQuant},
                {"MODa", IGenerateFile.ParamFileType.MODa},
                {"MODPlus", IGenerateFile.ParamFileType.MODPlus},
                {"MSAlign", IGenerateFile.ParamFileType.MSAlign},
                {"MSAlign_Histone", IGenerateFile.ParamFileType.MSAlignHistone},
                {"MSFragger", IGenerateFile.ParamFileType.MSFragger},
                {"MSGFPlus", IGenerateFile.ParamFileType.MSGFPlus},
                {"MSPathFinder", IGenerateFile.ParamFileType.MSPathFinder},
                {"Sequest", IGenerateFile.ParamFileType.BioWorks_Current},
                {"TopPIC", IGenerateFile.ParamFileType.TopPIC},
                {"XTandem", IGenerateFile.ParamFileType.X_Tandem}
            };

            if (toolNameToTypeMapping.TryGetValue(scriptName, out var paramFileType))
            {
                return paramFileType;
            }

            // Exact match not found; look for a partial match
            // For example, script MSGFPlus_MzML contains "MSGFPlus", so we'll return ParamFileType.MSGFPlus

            var scriptNameLCase = scriptName.ToLower();

            foreach (var entry in toolNameToTypeMapping)
            {
                if (scriptNameLCase.Contains(entry.Key.ToLower()))
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
        /// <param name="itemsToStore">Dictionary items to store as a packed job parameter</param>
        /// <param name="parameterName">Packed job parameter name</param>
        public void StorePackedJobParameterDictionary<T1, T2>(Dictionary<T1, T2> itemsToStore, string parameterName)
        {
            var packedJobParams = new List<string>();

            foreach (var item in itemsToStore)
            {
                packedJobParams.Add(item.Key + "=" + item.Value);
            }

            StorePackedJobParameterList(packedJobParams, parameterName);
        }

        /// <summary>
        /// Convert a string list to a packed job parameter (items are separated by tab characters)
        /// </summary>
        /// <param name="packedJobParams">Packed job parameters to store as param parameterName</param>
        /// <param name="parameterName">Packed job parameter name</param>
        protected void StorePackedJobParameterList(List<string> packedJobParams, string parameterName)
        {
            mJobParams.AddAdditionalParameter(AnalysisJob.JOB_PARAMETERS_SECTION, parameterName, Global.FlattenList(packedJobParams, "\t"));
        }

        /// <summary>
        /// Convert a string list to a packed job parameter (items are separated by tab characters)
        /// </summary>
        /// <param name="packedJobParams">Packed job parameters to store as param parameterName</param>
        /// <param name="parameterName">Packed job parameter name</param>
        protected void StorePackedJobParameterList(SortedSet<string> packedJobParams, string parameterName)
        {
            mJobParams.AddAdditionalParameter(AnalysisJob.JOB_PARAMETERS_SECTION, parameterName, Global.FlattenList(packedJobParams, "\t"));
        }

        /// <summary>
        /// Unzips all files in the specified Zip file
        /// </summary>
        /// <param name="zipFilePath">File to unzip</param>
        /// <param name="outputDirectoryPath">Target directory for the extracted files</param>
        /// <param name="callingFunctionName">calling method name (used for debugging purposes)</param>
        /// <returns>True if success, otherwise false</returns>
        public bool UnzipFileStart(string zipFilePath, string outputDirectoryPath, string callingFunctionName)
        {
            return FileSearchTool.UnzipFileStart(zipFilePath, outputDirectoryPath, callingFunctionName);
        }

        /// <summary>
        /// Create (or update) the .LastUsed file for the given FASTA file
        /// </summary>
        /// <remarks>The LastUsed file simply has the current date/time on the first line</remarks>
        /// <param name="fastaFile">FASTA file</param>
        private void UpdateLastUsedFile(FileInfo fastaFile)
        {
            FileSyncUtils.UpdateLastUsedFile(fastaFile);
        }

        /// <summary>
        /// Update mMessage, which is logged in the pipeline job steps table when the job step finishes
        /// </summary>
        /// <param name="statusMessage">New status message</param>
        /// <param name="appendToExisting">True to append to mMessage; false to overwrite it</param>
        public void UpdateStatusMessage(string statusMessage, bool appendToExisting = true)
        {
            if (appendToExisting)
            {
                mMessage = Global.AppendToComment(mMessage, statusMessage);
            }
            else
            {
                mMessage = statusMessage;
            }

            LogDebugMessage(mMessage);
        }

        /// <summary>
        /// Removes any spectra with 2 or fewer ions in a _DTA.txt file
        /// </summary>
        /// <param name="workDir">Folder with the CDTA file</param>
        /// <param name="inputFileName">CDTA filename</param>
        /// <returns>True if success, false if an error</returns>
        protected bool ValidateCDTAFileRemoveSparseSpectra(string workDir, string inputFileName)
        {
            var success = mCDTAUtilities.RemoveSparseSpectra(workDir, inputFileName);

            if (!success && string.IsNullOrEmpty(mMessage))
            {
                UpdateStatusMessage("mCDTAUtilities.RemoveSparseSpectra returned false");
            }

            return success;
        }

        /// <summary>
        /// Makes sure the specified _DTA.txt file has scan=x and cs=y tags in the parent ion line
        /// </summary>
        /// <param name="sourceFilePath">Input _DTA.txt file to parse</param>
        /// <param name="replaceSourceFile">If true, replaces the source file with and updated file</param>
        /// <param name="deleteSourceFileIfUpdated">
        /// Only valid if replaceSourceFile is true;
        /// If true, the source file is deleted if an updated version is created.
        /// If false, the source file is renamed to .old if an updated version is created.
        /// </param>
        /// <param name="outputFilePath">
        /// Output file path to use for the updated file; required if replaceSourceFile is false; ignored if replaceSourceFile is true
        /// </param>
        /// <returns>True if success, false if an error</returns>
        protected bool ValidateCDTAFileScanAndCSTags(string sourceFilePath, bool replaceSourceFile, bool deleteSourceFileIfUpdated, string outputFilePath)
        {
            var success = mCDTAUtilities.ValidateCDTAFileScanAndCSTags(sourceFilePath, replaceSourceFile, deleteSourceFileIfUpdated, outputFilePath);

            if (!success && string.IsNullOrEmpty(mMessage))
            {
                UpdateStatusMessage("mCDTAUtilities.ValidateCDTAFileScanAndCSTags returned false");
            }

            return success;
        }

        /// <summary>
        /// Condenses CDTA files that are over 2 GB in size
        /// </summary>
        /// <param name="workDir">Working directory</param>
        /// <param name="inputFileName">Input file name</param>
        protected bool ValidateCDTAFileSize(string workDir, string inputFileName)
        {
            var success = mCDTAUtilities.ValidateCDTAFileSize(workDir, inputFileName);

            if (!success && string.IsNullOrEmpty(mMessage))
            {
                UpdateStatusMessage("mCDTAUtilities.ValidateCDTAFileSize returned false");
            }

            return success;
        }

        /// <summary>
        /// Validate that data in a _dta.txt file is centroided
        /// </summary>
        /// <param name="cdtaPath">_dta.txt file path</param>
        public bool ValidateCDTAFileIsCentroided(string cdtaPath)
        {
            try
            {
                // Read the m/z values in the _dta.txt file
                // Examine the data in each spectrum to determine if it is centroided

                mSpectraTypeClassifier = new SpectraTypeClassifier.SpectrumTypeClassifier();
                RegisterEvents(mSpectraTypeClassifier);
                mSpectraTypeClassifier.ReadingSpectra += SpectraTypeClassifier_ReadingSpectra;

                var success = mSpectraTypeClassifier.CheckCDTAFile(cdtaPath);

                if (!success)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        LogError("SpectraTypeClassifier encountered an error while parsing the _dta.txt file");
                    }

                    return false;
                }

                var fractionCentroided = mSpectraTypeClassifier.FractionCentroided;

                var commentSuffix = string.Format(" ({0:N0} total spectra)", mSpectraTypeClassifier.TotalSpectra);

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
                    //   MS-GF+ will likely skip 90% of the spectra because they did not appear centroided
                    UpdateStatusMessage("MS-GF+ will likely skip " + ((1 - fractionCentroided) * 100).ToString("0") + "% of the spectra because they do not appear centroided");
                    LogMessage(mMessage + commentSuffix);
                    return false;
                }

                // None of the spectra are centroided; unable to process with MS-GF+
                UpdateStatusMessage(SPECTRA_ARE_NOT_CENTROIDED + " with MS-GF+");
                LogMessage(mMessage + commentSuffix, 0, true);
                return false;
            }
            catch (Exception ex)
            {
                LogError("Error in ValidateCDTAFileIsCentroided", ex);
                return false;
            }
        }

        /// <summary>
        /// Validate that the specified file exists and has at least one tab-delimited row with a numeric value in the first column
        /// </summary>
        /// <param name="filePath">Path to the file</param>
        /// <param name="fileDescription">File description, e.g. Synopsis</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <param name="skipHeaderRow">When true, assume the first row has column names</param>
        /// <returns>True if the file has data; otherwise false</returns>
        public static bool ValidateFileHasData(string filePath, string fileDescription, out string errorMessage, bool skipHeaderRow = false)
        {
            const int numericDataColIndex = 0;
            return ValidateFileHasData(filePath, fileDescription, out errorMessage, numericDataColIndex, skipHeaderRow);
        }

        /// <summary>
        /// Validate that the specified file exists and has at least one tab-delimited row with a numeric value in the column given by numericDataColIndex
        /// </summary>
        /// <remarks>Set numericDataColIndex to -1 to look for any data, optionally skipping the header row</remarks>
        /// <param name="filePath">Path to the file</param>
        /// <param name="fileDescription">File description, e.g. Synopsis</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <param name="numericDataColIndex">Index of the numeric data column; use -1 to simply look for any text in the file</param>
        /// <param name="skipHeaderRow">When true, assume the first row has column names</param>
        /// <returns>True if the file has data; otherwise false</returns>
        public static bool ValidateFileHasData(string filePath, string fileDescription, out string errorMessage, int numericDataColIndex, bool skipHeaderRow = false)
        {
            var dataFound = false;

            errorMessage = string.Empty;

            try
            {
                var dataFileInfo = new FileInfo(filePath);

                if (!dataFileInfo.Exists)
                {
                    errorMessage = fileDescription + " file not found: " + dataFileInfo.Name;
                    return false;
                }

                if (dataFileInfo.Length == 0)
                {
                    errorMessage = fileDescription + " file is empty (zero-bytes)";
                    return false;
                }

                // This counts the number of non-empty rows after the header line
                var rowCountAfterHeader = 0;

                // Open the file and confirm it has data rows
                using (var reader = new StreamReader(new FileStream(dataFileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var headerSkipped = false;

                    while (!reader.EndOfStream && !dataFound)
                    {
                        var dataLine = reader.ReadLine();

                        if (skipHeaderRow && !headerSkipped)
                        {
                            headerSkipped = true;
                            continue;
                        }

                        if (string.IsNullOrEmpty(dataLine))
                            continue;

                        rowCountAfterHeader++;

                        if (numericDataColIndex < 0)
                        {
                            dataFound = true;
                        }
                        else
                        {
                            // Split on the tab character and check if the first column is numeric
                            var dataCols = dataLine.Split('\t');

                            if (dataCols.Length <= numericDataColIndex)
                                continue;

                            if (double.TryParse(dataCols[numericDataColIndex], out _))
                            {
                                dataFound = true;
                            }
                        }
                    }
                }

                if (!dataFound)
                {
                    // Example messages:
                    //   MaxQuant msms.txt file is empty (no data)
                    //   MaxQuant msms.txt file is empty (no numeric data in column 2)

                    var messageDetail = numericDataColIndex < 0 || rowCountAfterHeader == 0
                        ? "no data"
                        : string.Format("no numeric data in column {0}", numericDataColIndex + 1);

                    errorMessage = string.Format("{0} file is empty ({1})", fileDescription, messageDetail);
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
        /// <remarks>
        /// Typical names for javaMemorySizeJobParamName are MSGFJavaMemorySize, MSGFPlusJavaMemorySize, MSDeconvJavaMemorySize, and FragPipeMemorySizeMB
        /// These parameters are loaded from DMS Settings Files (table T_Settings_Files in DMS5, copied to table T_Job_Parameters in DMS_Pipeline)
        /// </remarks>
        /// <param name="memorySizeJobParamName">Name of the job parameter that defines the amount of memory (in MB) that must be available on the system</param>
        /// <param name="logFreeMemoryOnSuccess">If true, post a log entry if sufficient memory is, in fact, available</param>
        /// <returns>True if sufficient free memory; false if not enough free memory</returns>
        protected bool ValidateFreeMemorySize(string memorySizeJobParamName, bool logFreeMemoryOnSuccess = true)
        {
            // Lookup parameter memorySizeJobParamName; assume 2000 MB if not defined
            var freeMemoryRequiredMB = mJobParams.GetJobParameter(memorySizeJobParamName, 2000);

            // Require freeMemoryRequiredMB be at least 0.5 GB
            if (freeMemoryRequiredMB < 512)
                freeMemoryRequiredMB = 512;

            if (mDebugLevel < 1)
                logFreeMemoryOnSuccess = false;

            var validFreeMemory = ValidateFreeMemorySize(freeMemoryRequiredMB, StepToolName, logFreeMemoryOnSuccess);

            if (!validFreeMemory)
                mInsufficientFreeMemory = true;

            return validFreeMemory;
        }

        /// <summary>
        /// Verify that the system has the specified amount of memory available
        /// </summary>
        /// <param name="freeMemoryRequiredMB">Free memory required, in MB</param>
        /// <param name="stepToolName">Step tool name</param>
        /// <param name="logFreeMemoryOnSuccess">If true, log the required memory and actual free memory</param>
        /// <returns>True if sufficient free memory; false if not enough free memory</returns>
        public static bool ValidateFreeMemorySize(int freeMemoryRequiredMB, string stepToolName, bool logFreeMemoryOnSuccess)
        {
            var freeMemoryMB = Global.GetFreeMemoryMB();

            if (freeMemoryRequiredMB >= freeMemoryMB)
            {
                var errMsg = string.Format(
                    "Not enough free memory to run {0}; need {1:N0} MB but system has {2:N0} MB available",
                    stepToolName, freeMemoryRequiredMB, freeMemoryMB);

                if (Global.RunningOnDeveloperComputer())
                {
                    LogTools.LogWarning(errMsg);
                    ConsoleMsgUtils.ShowWarning(errMsg);
                    ConsoleMsgUtils.SleepSeconds(2);
                    return true;
                }

                LogTools.LogError(errMsg);
                return false;
            }

            if (logFreeMemoryOnSuccess)
            {
                // Example messages:
                //   MS-GF+ will use 8192 MB; system has 7296 MB available
                //   MSFragger will use 23,552 MB; system has 90,696 MB available

                var message = string.Format(
                     "{0} will use {1:N0} MB; system has {2:N0} MB available",
                     stepToolName, freeMemoryRequiredMB, freeMemoryMB);

                LogTools.LogDebug(message);
            }

            return true;
        }

        /// <summary>
        /// Validate a FASTA file residing on an offline system
        /// Compares the CRC32 hash of the file to the .hashcheck file, creating or updating the .localhashcheck
        /// Skips the validation if the .localhashcheck file is less than 48 hours old
        /// </summary>
        /// <param name="fastaFile">FASTA file</param>
        /// <returns>True if valid, false if an error</returns>
        private bool ValidateOfflineFASTA(FileInfo fastaFile)
        {
            try
            {
                // Find the hashcheck file for this FASTA file
                if (fastaFile.Directory == null)
                {
                    LogError("Unable to determine the parent directory of " + fastaFile.FullName);
                    return false;
                }

                var hashcheckFileSpec = fastaFile.Name + "*" + OrganismDatabaseHandler.ProteinExport.GetFASTAFromDMS.HashcheckSuffix;
                var hashcheckFiles = fastaFile.Directory.GetFiles(hashcheckFileSpec);

                if (hashcheckFiles.Length == 0)
                {
                    LogError("FASTA validation error: hashcheck file not found for " + fastaFile.FullName);
                    return false;
                }

                var hashCheckFile = hashcheckFiles.First();

                var reCRC32 = new Regex(@"\.(?<CRC32>[^.]+)\.hashcheck$");

                var match = reCRC32.Match(hashCheckFile.Name);

                if (!match.Success)
                {
                    LogError("FASTA validation error: hashcheck filename not in the expected format, " + hashCheckFile.Name);
                    return false;
                }

                var expectedHash = match.Groups["CRC32"].Value;
                var crc32Hash = expectedHash;

                var fastaTools = new OrganismDatabaseHandler.ProteinExport.GetFASTAFromDMS();
                RegisterEvents(fastaTools);

                var fastaIsValid = fastaTools.ValidateMatchingHash(
                    fastaFile.FullName,
                    ref crc32Hash,
                    retryHoldoffHours: 48,
                    forceRegenerateHash: false,
                    hashcheckExtension: LOCALHASHCHECK_EXTENSION);

                if (fastaIsValid)
                    return true;

                LogError("FASTA validation error: hash validation failed for " + fastaFile.FullName);

                if (!string.Equals(expectedHash, crc32Hash))
                    LogWarning("For {0}, expected hash {1} but actually {2}", fastaFile.Name, expectedHash, crc32Hash);

                return false;
            }
            catch (Exception ex)
            {
                LogError("FASTA validation error: " + ex.Message, ex);
                return false;
            }
        }

        private void CDTAUtilities_ProgressEvent(string taskDescription, float percentComplete)
        {
            if (mDebugLevel >= 1)
            {
                if (mDebugLevel == 1 && DateTime.UtcNow.Subtract(mLastCDTAUtilitiesUpdateTime).TotalSeconds >= 60 ||
                    mDebugLevel > 1 && DateTime.UtcNow.Subtract(mLastCDTAUtilitiesUpdateTime).TotalSeconds >= 20)
                {
                    mLastCDTAUtilitiesUpdateTime = DateTime.UtcNow;

                    LogDebugMessage(" ... CDTAUtilities: " + percentComplete.ToString("0.00") + "% complete");
                }
            }
        }

        private void FastaTools_FileGenerationCompleted(string fullOutputPath)
        {
            if (mDebugLevel >= 1)
            {
                LogDebugMessage(string.Format("FASTA generation: created FASTA file {0}", fullOutputPath));
            }

            // Get the name of the FASTA file that was generated
            mFastaFileName = Path.GetFileName(fullOutputPath);
        }

        private void FastaTools_FileGenerationProgress(string statusMsg, double fractionDone)
        {
            const int MINIMUM_LOG_INTERVAL_SEC = 10;

            var forceLog = mDebugLevel >= 1 && statusMsg.Contains(OrganismDatabaseHandler.ProteinExport.GetFASTAFromDMS.LockFileProgressText);

            if (mDebugLevel >= 3 || forceLog)
            {
                // Limit the logging to once every MINIMUM_LOG_INTERVAL_SEC seconds
                if (forceLog || DateTime.UtcNow.Subtract(mFastaToolsLastLogTime).TotalSeconds >= MINIMUM_LOG_INTERVAL_SEC ||
                    fractionDone - mFastaToolFractionDoneSaved >= 0.25)
                {
                    mFastaToolsLastLogTime = DateTime.UtcNow;
                    mFastaToolFractionDoneSaved = fractionDone;
                    LogDebugMessage("Generating FASTA file, " + (fractionDone * 100).ToString("0.0") + "% complete, " + statusMsg);
                }
            }
        }

        private void FastaTools_FileGenerationStarted(string taskMsg)
        {
            if (mDebugLevel >= 1)
            {
                LogDebugMessage(string.Format("FASTA generation: {0}", taskMsg));
            }
        }

        /// <summary>
        /// Progress update
        /// </summary>
        /// <param name="progressMessage">Progress message</param>
        /// <param name="percentComplete">Value between 0 and 100</param>
        private void SplitFastaFileUtility_ProgressUpdate(string progressMessage, float percentComplete)
        {
            if (mDebugLevel < 1 ||
                mDebugLevel == 1 && DateTime.UtcNow.Subtract(mSplitFastaLastUpdateTime).TotalSeconds < 60 ||
                mDebugLevel > 1 && DateTime.UtcNow.Subtract(mSplitFastaLastUpdateTime).TotalSeconds < 20 ||
                percentComplete >= 100 && mSplitFastaLastPercentComplete >= 100)
            {
                return;
            }

            mSplitFastaLastUpdateTime = DateTime.UtcNow;
            mSplitFastaLastPercentComplete = percentComplete;

            if (percentComplete > 0)
            {
                LogDebugMessage(" ... " + progressMessage + ", " + percentComplete.ToString("0.0") + "% complete");
            }
            else
            {
                LogDebugMessage(" ... SplitFastaFile: " + progressMessage);
            }
        }

        private void SplitFastaFileUtility_SplittingBaseFastaFile(string baseFastaFileName, int numSplitParts)
        {
            LogDebugMessage("Splitting " + baseFastaFileName + " into " + numSplitParts + " parts");
        }

        private void FileCopyUtilities_CopyWithLocksComplete(DateTime startTimeUtc, string destinationFilePath)
        {
            LogCopyStats(startTimeUtc, destinationFilePath);
        }

        private void FileCopyUtilities_ResetTimestampForQueueWaitTime()
        {
            ResetTimestampForQueueWaitTimeLogging();
        }

        private void SpectraTypeClassifier_ReadingSpectra(int spectraProcessed)
        {
            LogDebugMessage(" ... " + spectraProcessed + " spectra parsed in the _dta.txt file");
        }
    }
}
