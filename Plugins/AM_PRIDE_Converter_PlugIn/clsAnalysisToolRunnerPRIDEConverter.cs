using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;
using AnalysisManagerBase;
using MyEMSLReader;
using PHRPReader;

namespace AnalysisManagerPRIDEConverterPlugIn
{
    /// <summary>
    /// Class for running PRIDEConverter
    /// </summary>
    /// <remarks>
    /// Although this class was originally created to prepare data for submission to PRIDE,
    /// we now primarily use it to submit data to ProteomeXchange
    /// </remarks>
    public class clsAnalysisToolRunnerPRIDEConverter : clsAnalysisToolRunnerBase
    {
        #region "Constants"

        private const string DOT_MGF = clsAnalysisResources.DOT_MGF_EXTENSION;
        private const string DOT_MZID_GZ = ".mzid.gz";
        private const string DOT_MZML = clsAnalysisResources.DOT_MZML_EXTENSION;
        private const string DOT_MZML_GZ = clsAnalysisResources.DOT_MZML_EXTENSION + clsAnalysisResources.DOT_GZ_EXTENSION;

        #endregion

        #region "Module Variables"

        private const string PRIDEConverter_CONSOLE_OUTPUT = "PRIDEConverter_ConsoleOutput.txt";

        /// <summary>
        /// Percent complete to report when the tool starts
        /// </summary>
        public const float PROGRESS_PCT_TOOL_RUNNER_STARTING = 20;

        private const float PROGRESS_PCT_SAVING_RESULTS = 95;
        private const float PROGRESS_PCT_COMPLETE = 99;

        private const string FILE_EXTENSION_PSEUDO_MSGF = ".msgf";
        private const string FILE_EXTENSION_MSGF_REPORT_XML = ".msgf-report.xml";
        private const string FILE_EXTENSION_MSGF_PRIDE_XML = ".msgf-pride.xml";

        private const string PARTIAL_SUBMISSION = "PARTIAL";
        private const string COMPLETE_SUBMISSION = "COMPLETE";

        private const string PNNL_NAME_COUNTRY = "Pacific Northwest National Laboratory";

        private const string DEFAULT_TISSUE_CV = "[PRIDE, PRIDE:0000442, Tissue not applicable to dataset, ]";
        private const string DEFAULT_TISSUE_CV_MOUSE_HUMAN = "[BTO, BTO:0000089, blood, ]";

        private const string DEFAULT_CELL_TYPE_CV = "[CL, CL:0000081, blood cell, ]";
        private const string DEFAULT_DISEASE_TYPE_CV = "[DOID, DOID:1612, breast cancer, ]";
        private const string DEFAULT_QUANTIFICATION_TYPE_CV = "[PRIDE, PRIDE:0000436, Spectral counting,]";
        private const string DELETION_WARNING = " -- If you delete this line, assure that the corresponding column values on the SME rows are empty " +
                                                "(leave the 'cell_type' and 'disease' column headers on the SMH line, " +
                                                "but assure that the SME lines have blank entries for this column)";

        private const double DEFAULT_PVALUE_THRESHOLD = 0.05;

        private string mConsoleOutputErrorMsg;

        // This dictionary tracks the peptide hit jobs defined for this data package
        // The keys are job numbers and the values contain job info
        private Dictionary<int, clsDataPackageJobInfo> mDataPackagePeptideHitJobs;

        private string mPrideConverterProgLoc = string.Empty;

        private readonly string mJavaProgLoc = string.Empty;
        private string mMSXmlGeneratorAppPath = string.Empty;

        private bool mCreateMSGFReportFilesOnly;
        private bool mCreateMGFFiles;
        private bool mCreatePrideXMLFiles;

        private bool mIncludePepXMLFiles;
        private bool mProcessMzIdFiles;

        private string mCacheFolderPath = string.Empty;
        private string mPreviousDatasetName = string.Empty;

        /// <summary>
        /// Previous dataset files to delete
        /// </summary>
        /// <remarks>Full file paths for files that will be deleted from the local work directory</remarks>
        private List<string> mPreviousDatasetFilesToDelete;

        /// <summary>
        /// Previous dataset files to copy
        /// </summary>
        /// <remarks>Full file paths for files that will be copied from the local work directory to the transfer directory</remarks>
        private List<string> mPreviousDatasetFilesToCopy;

        /// <summary>
        /// Cached FASTA file name
        /// </summary>
        private string mCachedOrgDBName = string.Empty;

        /// <summary>
        /// Cached proteins
        /// </summary>
        /// <remarks>
        /// Keys are protein name
        /// Values are key-value pairs where the key is the Protein Index and the value is the protein sequence
        /// </remarks>
        private Dictionary<string, KeyValuePair<int, string>> mCachedProteins;

        /// <summary>
        /// Protein PSM Counts
        /// </summary>
        /// <remarks>
        /// Keys are protein index in mCachedProteins
        /// Values are the filter-passing PSMs for each protein
        /// </remarks>
        private Dictionary<int, int> mCachedProteinPSMCounts;

        /// <summary>
        /// Master px file list
        /// </summary>
        /// <remarks>
        /// Keys are filenames
        /// Values contain info on each file
        /// Note that PRIDE uses case-sensitive file names, so it is important to properly capitalize the files to match the official DMS dataset name
        /// However, this dictionary is instantiated with a case-insensitive comparer, to prevent duplicate entries
        /// </remarks>
        private Dictionary<string, clsPXFileInfoBase> mPxMasterFileList;

        /// <summary>
        /// Px result files
        /// </summary>
        /// <remarks>
        /// Keys are PXFileIDs
        /// Values contain info on each file, including the PXFileType and the FileIDs that map to this file (empty list if no mapped files)
        /// </remarks>
        private Dictionary<int, clsPXFileInfo> mPxResultFiles;

        private udtFilterThresholdsType mFilterThresholdsUsed;

        /// <summary>
        /// Instrument group names
        /// </summary>
        /// <remarks>
        /// Keys are instrument group names
        /// Values are the specific instrument names in the instrument gruop
        /// </remarks>
        private Dictionary<string, SortedSet<string>> mInstrumentGroupsStored;

        /// <summary>
        /// Search tool names
        /// </summary>
        private SortedSet<string> mSearchToolsUsed;

        /// <summary>
        /// Experiment organism information
        /// </summary>
        /// <remarks>
        /// Keys are NEWT IDs
        /// Values are the NEWT name for the given ID
        /// </remarks>
        private Dictionary<int, string> mExperimentNEWTInfo;

        /// <summary>
        /// List of BTO Tissue IDs for experiments in the data package
        /// </summary>
        /// <remarks>
        /// Keys are BTO IDs (e.g., BTO:0000131)
        /// Values are tissue names (e.g., blood plasma)
        /// </remarks>
        private Dictionary<string, string> mExperimentTissue;

        /// <summary>
        /// Modifications used in the analysis jobs
        /// </summary>
        /// <remarks>
        /// Keys are Unimod accession names(e.g.UNIMOD:35)
        /// Values are CvParam data for the modification
        /// </remarks>
        private Dictionary<string, clsSampleMetadata.udtCvParamInfoType> mModificationsUsed;

        /// <summary>
        /// Sample info for each mzid.gz file
        /// (instantiated with a case insensitive comparer)
        /// </summary>
        /// <remarks>
        /// Keys are mzid.gz file names
        /// Values are the sample info for the file
        /// </remarks>
        private Dictionary<string, clsSampleMetadata> mMzIdSampleInfo;

        /// <summary>
        /// _dta.txt file stats
        /// </summary>
        /// <remarks>
        /// Keys are _dta.txt file names
        /// Values contain info on each file
        /// </remarks>
        private Dictionary<string, clsPXFileInfoBase> mCDTAFileStats;

        /// <summary>
        /// MzML /  MzXML file creator
        /// </summary>
        private AnalysisManagerMsXmlGenPlugIn.clsMSXMLCreator mMSXmlCreator;

        /// <summary>
        /// DTA to MGF converter
        /// </summary>
        private DTAtoMGF.clsDTAtoMGF mDTAtoMGF;

        /// <summary>
        /// Program runner
        /// </summary>
        private clsRunDosProgram mCmdRunner;

        #endregion

        #region "Structures and Enums"

        private struct udtFilterThresholdsType
        {
            public float PValueThreshold;
            public float FDRThreshold;
            public float PepFDRThreshold;
            public float MSGFSpecEValueThreshold;
            public bool UseFDRThreshold;
            public bool UsePepFDRThreshold;
            public bool UseMSGFSpecEValue;

            public void Clear()
            {
                PValueThreshold = (float)DEFAULT_PVALUE_THRESHOLD;
                UseFDRThreshold = false;
                UsePepFDRThreshold = false;
                UseMSGFSpecEValue = true;
                FDRThreshold = 0.01f;
                PepFDRThreshold = 0.01f;
                MSGFSpecEValueThreshold = 1E-09f;
            }
        }

        private struct udtPseudoMSGFDataType
        {
            public int ResultID;
            public string Peptide;
            public string CleanSequence;
            public string PrefixResidue;
            public string SuffixResidue;
            public int ScanNumber;
            public short ChargeState;
            public string PValue;
            public string MQScore;
            public string TotalPRMScore;
            public short NTT;
            public string MSGFSpecEValue;
            public string DeltaScore;
            public string DeltaScoreOther;
            public string Protein;

            public override string ToString()
            {
                if (string.IsNullOrWhiteSpace(Peptide))
                    return "ResultID " + ResultID;

                return "ResultID " + ResultID + ": " + Peptide;
            }
        }

        private enum eMSGFReportXMLFileLocation
        {
            Header = 0,
            SearchResultIdentifier = 1,
            Metadata = 2,
            Protocol = 3,
            MzDataAdmin = 4,
            MzDataInstrument = 5,
            MzDataDataProcessing = 6,
            ExperimentAdditional = 7,
            Identifications = 8,
            PTMs = 9,
            DatabaseMappings = 10,
            ConfigurationOptions = 11
        }

        private enum eMzIDXMLFileLocation
        {
            Header = 0,
            SequenceCollection = 1,
            AnalysisCollection = 2,
            AnalysisProtocolCollection = 3,
            DataCollection = 4,
            Inputs = 5,
            InputSearchDatabase = 6,
            InputSpectraData = 7,
            AnalysisData = 8
        }

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs PRIDEConverter tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            try
            {
                // Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (m_DebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerPRIDEConverter.RunTool(): Enter");
                }

                // Verify that program files exist
                if (!DefineProgramPaths())
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the PRIDE Converter version info in the database
                if (!StoreToolVersionInfo(mPrideConverterProgLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining PRIDE Converter version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mConsoleOutputErrorMsg = string.Empty;

                mCacheFolderPath = m_jobParams.GetJobParameter("CacheFolderPath", clsAnalysisResourcesPRIDEConverter.DEFAULT_CACHE_FOLDER_PATH);

                LogMessage("Running PRIDEConverter");

                // Initialize dataPackageDatasets
                if (!LoadDataPackageDatasetInfo(out var dataPackageDatasets))
                {
                    var msg = "Error loading data package dataset info";
                    LogError(msg + ": clsAnalysisToolRunnerBase.LoadDataPackageDatasetInfo returned false");
                    m_message = msg;
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Initialize mDataPackagePeptideHitJobs
                if (!LookupDataPackagePeptideHitJobs())
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Monitor the FileDownloaded event in this class
                m_MyEMSLUtilities.FileDownloaded += m_MyEMSLDatasetListInfo_FileDownloadedEvent;

                // The analysisResults object is used to copy files to/from this computer
                var analysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);

                // Assure that the remote transfer folder exists
                var remoteTransferFolder = CreateRemoteTransferFolder(analysisResults, mCacheFolderPath);

                try
                {
                    // Create the remote Transfer Directory
                    analysisResults.CreateFolderWithRetry(remoteTransferFolder, maxRetryCount: 5, retryHoldoffSeconds: 20, increaseHoldoffOnEachRetry: true);
                }
                catch (Exception ex)
                {
                    // Folder creation error
                    LogError("Exception creating transfer directory folder", ex);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Read the PX_Submission_Template.px file
                var templateParameters = ReadTemplatePXSubmissionFile();

                var jobFailureCount = ProcessJobs(analysisResults, remoteTransferFolder, templateParameters, dataPackageDatasets);

                // Create the PX Submission file
                var success = CreatePXSubmissionFile(templateParameters);

                m_progress = PROGRESS_PCT_COMPLETE;
                m_StatusTools.UpdateAndWrite(m_progress);

                if (success)
                {
                    if (m_DebugLevel >= 3)
                    {
                        LogDebug("PRIDEConverter Complete");
                    }
                }

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                PRISM.clsProgRunner.GarbageCollectNow();

                if (!success || jobFailureCount > 0)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                DefineFilesToSkipTransfer();

                var copySuccess = CopyResultsToTransferDirectory(mCacheFolderPath);

                return copySuccess ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;


            }
            catch (Exception ex)
            {
                LogError("Exception in PRIDEConverterPlugin->RunTool", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        /// <summary>
        /// Process the analysis jobs in mDataPackagePeptideHitJobs
        /// </summary>
        /// <param name="analysisResults"></param>
        /// <param name="remoteTransferFolder"></param>
        /// <param name="templateParameters"></param>
        /// <param name="dataPackageDatasets">Datasets in the data package (keys are DatasetID)</param>
        /// <returns></returns>
        private int ProcessJobs(
            clsAnalysisResults analysisResults,
            string remoteTransferFolder,
            IReadOnlyDictionary<string, string> templateParameters,
            IReadOnlyDictionary<int, clsDataPackageDatasetInfo> dataPackageDatasets)
        {
            var jobsProcessed = 0;
            var jobFailureCount = 0;

            try
            {
                // Initialize the class-wide variables
                var udtFilterThresholds = InitializeOptions();

                // Extract the dataset raw file paths
                var datasetRawFilePaths = ExtractPackedJobParameterDictionary(clsAnalysisResources.JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS);

                // Process each job in mDataPackagePeptideHitJobs
                // Sort the jobs by dataset so that we can use the same .mzXML file for datasets with multiple jobs
                var linqJobsSortedByDataset = (from item in mDataPackagePeptideHitJobs orderby item.Value.Dataset, SortPreference(item.Value.Tool) select item);

                var assumeInstrumentDataUnpurged = m_jobParams.GetJobParameter("AssumeInstrumentDataUnpurged", true);

                const bool continueOnError = true;
                const int maxErrorCount = 10;
                var dtLastLogTime = DateTime.UtcNow;

                // This dictionary tracks the datasets that have been processed
                // Keys are dataset ID, values are dataset name
                var datasetsProcessed = new Dictionary<int, string>();

                foreach (var jobInfo in linqJobsSortedByDataset)
                {
                    var udtCurrentJobInfo = jobInfo.Value;

                    m_StatusTools.CurrentOperation = "Processing job " + udtCurrentJobInfo.Job + ", dataset " + udtCurrentJobInfo.Dataset;

                    Console.WriteLine();
                    LogDebug(string.Format("{0}: {1}", jobsProcessed + 1, m_StatusTools.CurrentOperation), 10);

                    var result = ProcessJob(
                        jobInfo, udtFilterThresholds,
                        analysisResults, dataPackageDatasets,
                        remoteTransferFolder, datasetRawFilePaths,
                        templateParameters, assumeInstrumentDataUnpurged);

                    if (result != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        jobFailureCount += 1;
                        if (!continueOnError || jobFailureCount > maxErrorCount)
                            break;
                    }

                    if (!datasetsProcessed.ContainsKey(udtCurrentJobInfo.DatasetID))
                    {
                        datasetsProcessed.Add(udtCurrentJobInfo.DatasetID, udtCurrentJobInfo.Dataset);
                    }

                    jobsProcessed += 1;
                    m_progress = ComputeIncrementalProgress(PROGRESS_PCT_TOOL_RUNNER_STARTING, PROGRESS_PCT_SAVING_RESULTS, jobsProcessed,
                        mDataPackagePeptideHitJobs.Count);
                    m_StatusTools.UpdateAndWrite(m_progress);

                    if (DateTime.UtcNow.Subtract(dtLastLogTime).TotalMinutes >= 5 || m_DebugLevel >= 2)
                    {
                        dtLastLogTime = DateTime.UtcNow;
                        LogDebug(" ... processed " + jobsProcessed + " / " + mDataPackagePeptideHitJobs.Count + " jobs");
                    }
                }

                TransferPreviousDatasetFiles(analysisResults, remoteTransferFolder);

                // Look for datasets associated with the data package that have no PeptideHit jobs
                // Create fake PeptideHit jobs in the .px file to alert the user of the missing jobs

                foreach (var datasetInfo in dataPackageDatasets)
                {
                    var datasetId = datasetInfo.Key;
                    var datasetName = datasetInfo.Value.Dataset;

                    if (datasetsProcessed.ContainsKey(datasetId))
                        continue;

                    m_StatusTools.CurrentOperation = "Adding dataset " + datasetName + " (no associated PeptideHit job)";

                    Console.WriteLine();
                    LogDebug(m_StatusTools.CurrentOperation, 10);

                    AddPlaceholderDatasetEntry(datasetInfo);
                }

                // If we were still unable to delete some files, we want to make sure that they don't end up in the results folder
                foreach (var fileToDelete in mPreviousDatasetFilesToDelete)
                {
                    m_jobParams.AddResultFileToSkip(fileToDelete);
                }

                return jobFailureCount;
            }
            catch (Exception ex)
            {
                LogError("Exception in ProcessJobs", ex);
                var totalFailedJobs = jobFailureCount + mDataPackagePeptideHitJobs.Count - jobsProcessed;
                return totalFailedJobs;
            }
        }

        private int SortPreference(string tool)
        {
            if (tool.ToLower().StartsWith("msgfplus"))
                return 0;

            if (tool.ToLower().StartsWith("xtandem"))
                return 1;

            return 5;
        }

        private void AddExperimentTissueId(string tissueId, string tissueName)
        {
            if (!string.IsNullOrWhiteSpace(tissueId) && !mExperimentTissue.ContainsKey(tissueId))
            {
                mExperimentTissue.Add(tissueId, tissueName);
            }
        }

        private void AddNEWTInfo(int newtID, string newtName)
        {
            if (newtID == 0)
            {
                newtID = 2323;
                newtName = "unclassified Bacteria";
            }

            if (!mExperimentNEWTInfo.ContainsKey(newtID))
            {
                mExperimentNEWTInfo.Add(newtID, newtName);
            }
        }

        private void AddPlaceholderDatasetEntry(KeyValuePair<int, clsDataPackageDatasetInfo> datasetInfo)
        {
            AddExperimentTissueId(datasetInfo.Value.Experiment_Tissue_ID, datasetInfo.Value.Experiment_Tissue_Name);

            AddNEWTInfo(datasetInfo.Value.Experiment_NEWT_ID, datasetInfo.Value.Experiment_NEWT_Name);

            // Store the instrument group and instrument name
            StoreInstrumentInfo(datasetInfo.Value);

            var udtDatasetInfo = datasetInfo.Value;
            var datasetRawFilePath = Path.Combine(udtDatasetInfo.ServerStoragePath, udtDatasetInfo.Dataset + ".raw");

            var dataPkgJob = clsAnalysisResources.GetPseudoDataPackageJobInfo(udtDatasetInfo);

            var rawFileID = AddPxFileToMasterList(datasetRawFilePath, dataPkgJob);

            AddPxResultFile(rawFileID, clsPXFileInfoBase.ePXFileType.Raw, datasetRawFilePath, dataPkgJob);
        }

        private int AddPxFileToMasterList(string filePath, clsDataPackageJobInfo dataPkgJob)
        {
            var fiFile = new FileInfo(filePath);

            if (mPxMasterFileList.TryGetValue(fiFile.Name, out var oPXFileInfo))
            {
                // File already exists
                return oPXFileInfo.FileID;
            }

            var filename = CheckFilenameCase(fiFile, dataPkgJob.Dataset);

            oPXFileInfo = new clsPXFileInfoBase(filename, dataPkgJob)
            {
                FileID = mPxMasterFileList.Count + 1
            };


            if (fiFile.Exists)
            {
                oPXFileInfo.Length = fiFile.Length;
                oPXFileInfo.MD5Hash = string.Empty;      // Don't compute the hash; it's not needed
            }
            else
            {
                oPXFileInfo.Length = 0;
                oPXFileInfo.MD5Hash = string.Empty;
            }

            mPxMasterFileList.Add(fiFile.Name, oPXFileInfo);

            return oPXFileInfo.FileID;
        }

        private bool AddPxResultFile(int fileId, clsPXFileInfoBase.ePXFileType eFileType, string filePath, clsDataPackageJobInfo dataPkgJob)
        {
            var fiFile = new FileInfo(filePath);

            if (mPxResultFiles.TryGetValue(fileId, out var oPXFileInfo))
            {
                // File already defined in the mapping list
                return true;
            }

            if (!mPxMasterFileList.TryGetValue(fiFile.Name, out var oMasterPXFileInfo))
            {
                // File not found in mPxMasterFileList, we cannot add the mapping
                LogError("File " + fiFile.Name + " not found in mPxMasterFileList; unable to add to mPxResultFiles");
                return false;
            }

            if (oMasterPXFileInfo.FileID != fileId)
            {
                var msg = "FileID mismatch for " + fiFile.Name;
                LogError(msg + ":  mPxMasterFileList.FileID = " + oMasterPXFileInfo.FileID + " vs. FileID " + fileId + " passed into AddPxFileToMapping");
                m_message = msg;
                return false;
            }

            var filename = CheckFilenameCase(fiFile, dataPkgJob.Dataset);

            oPXFileInfo = new clsPXFileInfo(filename, dataPkgJob);
            oPXFileInfo.Update(oMasterPXFileInfo);
            oPXFileInfo.PXFileType = eFileType;

            mPxResultFiles.Add(fileId, oPXFileInfo);

            return true;
        }

        /// <summary>
        /// Adds value to listToUpdate only if the value is not yet present in the list
        /// </summary>
        /// <param name="listToUpdate"></param>
        /// <param name="value"></param>
        /// <remarks></remarks>
        private void AddToListIfNew(ICollection<string> listToUpdate, string value)
        {
            if (!listToUpdate.Contains(value))
            {
                listToUpdate.Add(value);
            }
        }

        private bool AppendToPXFileInfo(clsDataPackageJobInfo dataPkgJob, IReadOnlyDictionary<string, string> datasetRawFilePaths,
            clsResultFileContainer resultFiles)
        {
            // Add the files to be submitted to ProteomeXchange to the master file list
            // In addition, append new mappings to the ProteomeXchange mapping list

            var prideXMLFileId = 0;
            if (!string.IsNullOrEmpty(resultFiles.PrideXmlFilePath))
            {
                AddToListIfNew(mPreviousDatasetFilesToCopy, resultFiles.PrideXmlFilePath);

                prideXMLFileId = AddPxFileToMasterList(resultFiles.PrideXmlFilePath, dataPkgJob);
                if (!AddPxResultFile(prideXMLFileId, clsPXFileInfoBase.ePXFileType.Result, resultFiles.PrideXmlFilePath, dataPkgJob))
                {
                    return false;
                }
            }

            var rawFileID = 0;
            if (datasetRawFilePaths.TryGetValue(dataPkgJob.Dataset, out var datasetRawFilePath))
            {
                if (!string.IsNullOrEmpty(datasetRawFilePath))
                {
                    rawFileID = AddPxFileToMasterList(datasetRawFilePath, dataPkgJob);
                    if (!AddPxResultFile(rawFileID, clsPXFileInfoBase.ePXFileType.Raw, datasetRawFilePath, dataPkgJob))
                    {
                        return false;
                    }

                    if (prideXMLFileId > 0)
                    {
                        if (!DefinePxFileMapping(prideXMLFileId, rawFileID))
                        {
                            return false;
                        }
                    }
                }
            }

            var peakFileId = 0;
            if (!string.IsNullOrEmpty(resultFiles.MGFFilePath))
            {
                AddToListIfNew(mPreviousDatasetFilesToCopy, resultFiles.MGFFilePath);

                peakFileId = AddPxFileToMasterList(resultFiles.MGFFilePath, dataPkgJob);
                if (!AddPxResultFile(peakFileId, clsPXFileInfoBase.ePXFileType.Peak, resultFiles.MGFFilePath, dataPkgJob))
                {
                    return false;
                }

                if (prideXMLFileId == 0)
                {
                    // Pride XML file was not created
                    if (rawFileID > 0 && resultFiles.MzIDFilePaths.Count == 0)
                    {
                        // Only associate Peak files with .Raw files if we do not have a .mzid.gz file
                        if (!DefinePxFileMapping(peakFileId, rawFileID))
                        {
                            return false;
                        }
                    }
                }
                else
                {
                    // Pride XML file was created
                    if (!DefinePxFileMapping(prideXMLFileId, peakFileId))
                    {
                        return false;
                    }
                }
            }

            foreach (var mzIdResultFile in resultFiles.MzIDFilePaths)
            {
                var success = AddMzidOrPepXmlFileToPX(dataPkgJob, mzIdResultFile, clsPXFileInfoBase.ePXFileType.ResultMzId, prideXMLFileId,
                    rawFileID, peakFileId);
                if (!success)
                    return false;
            }

            if (!string.IsNullOrWhiteSpace(resultFiles.PepXMLFile))
            {
                var success = AddMzidOrPepXmlFileToPX(dataPkgJob, resultFiles.PepXMLFile, clsPXFileInfoBase.ePXFileType.Search, prideXMLFileId,
                    rawFileID, peakFileId);
                if (!success)
                    return false;
            }

            return true;
        }

        private bool AddMzidOrPepXmlFileToPX(clsDataPackageJobInfo dataPkgJob, string resultFilePath, clsPXFileInfoBase.ePXFileType ePxFileType,
            int prideXMLFileId, int rawFileID, int peakFileId)
        {
            AddToListIfNew(mPreviousDatasetFilesToCopy, resultFilePath);

            var dataFileID = AddPxFileToMasterList(resultFilePath, dataPkgJob);
            if (!AddPxResultFile(dataFileID, ePxFileType, resultFilePath, dataPkgJob))
            {
                return false;
            }

            if (prideXMLFileId == 0)
            {
                // Pride XML file was not created
                if (peakFileId > 0)
                {
                    if (!DefinePxFileMapping(dataFileID, peakFileId))
                    {
                        return false;
                    }
                }

                if (rawFileID > 0)
                {
                    if (!DefinePxFileMapping(dataFileID, rawFileID))
                    {
                        return false;
                    }
                }
            }
            else
            {
                // Pride XML file was created
                if (!DefinePxFileMapping(prideXMLFileId, dataFileID))
                {
                    return false;
                }
            }

            return true;
        }

        private string CheckFilenameCase(FileSystemInfo fiFile, string dataset)
        {
            var filename = fiFile.Name;

            if (!string.IsNullOrEmpty(fiFile.Extension))
            {
                var fileBaseName = Path.GetFileNameWithoutExtension(fiFile.Name);

                if (fileBaseName.ToLower().StartsWith(dataset.ToLower()))
                {
                    if (!fileBaseName.StartsWith(dataset))
                    {
                        // Case-mismatch; fix it
                        if (fileBaseName.Length == dataset.Length)
                        {
                            fileBaseName = dataset;
                        }
                        else
                        {
                            fileBaseName = dataset + fileBaseName.Substring(dataset.Length);
                        }
                    }
                }

                if (fiFile.Extension.Equals(DOT_MZML, StringComparison.OrdinalIgnoreCase))
                {
                    filename = fileBaseName + DOT_MZML;
                }
                else if (fiFile.Extension.Equals(DOT_MZML_GZ, StringComparison.OrdinalIgnoreCase))
                {
                    filename = fileBaseName + DOT_MZML_GZ;
                }
                else
                {
                    filename = fileBaseName + fiFile.Extension.ToLower();
                }
            }

            return filename;
        }

        private double ComputeApproximateEValue(double msgfSpecEValue)
        {
            var eValueEstimate = msgfSpecEValue;

            if (msgfSpecEValue < 1E-106)
                return 1E-100;

            try
            {
                // Estimate Log10(EValue) using 10^(Log10(SpecEValue) x 0.9988 + 6.43)
                // This equation also works for estimating PValue given SpecProb,
                // and was originally determined using Job 893431 for dataset QC_Shew_12_02_0pt25_Frac-08_7Nov12_Tiger_12-09-36
                eValueEstimate = Math.Log10(msgfSpecEValue) * 0.9988 + 6.43;
                eValueEstimate = Math.Pow(10, eValueEstimate);
            }
            catch (Exception)
            {
                // Ignore errors here
                // We will simply return the EValue estimate
            }

            return eValueEstimate;
        }

        /// <summary>
        /// Convert the _dta.txt file to a .mgf file
        /// </summary>
        /// <param name="dataPkgJob"></param>
        /// <param name="mgfFilePath">Output parameter: path of the newly created .mgf file</param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool ConvertCDTAToMGF(clsDataPackageJobInfo dataPkgJob, out string mgfFilePath)
        {
            mgfFilePath = string.Empty;

            try
            {
                mDTAtoMGF = new DTAtoMGF.clsDTAtoMGF
                {
                    Combine2And3PlusCharges = false,
                    FilterSpectra = false,
                    MaximumIonsPer100MzInterval = 40,
                    NoMerge = true
                };
                mDTAtoMGF.ErrorEvent += mDTAtoMGF_ErrorEvent;

                // Convert the _dta.txt file for this dataset
                var fiCDTAFile = new FileInfo(Path.Combine(m_WorkDir, dataPkgJob.Dataset + "_dta.txt"));

                if (!fiCDTAFile.Exists)
                {
                    var msg = "_dta.txt file not found for job " + dataPkgJob.Job;
                    LogError(msg + ": " + fiCDTAFile.FullName);
                    m_message = msg;
                    return false;
                }

                // Compute the MD5 hash for this _dta.txt file
                var md5Hash = PRISM.HashUtilities.ComputeFileHashMD5(fiCDTAFile.FullName);

                // Make sure this is either a new _dta.txt file or identical to a previous one
                // Abort processing if the job list contains multiple jobs for the same dataset but those jobs used different _dta.txt files
                // However, if one of the jobs is Sequest and one is MSGF+, preferentially use the _dta.txt file from the MSGF+ job

                if (mCDTAFileStats.TryGetValue(fiCDTAFile.Name, out var existingFileInfo))
                {
                    if (existingFileInfo.JobInfo.Tool.ToLower().StartsWith("msgf"))
                    {
                        // Existing job found, but it's a MSGF+ job (which is fully supported by PRIDE)
                        // Just use the existing .mgf file
                        return true;
                    }

                    if (fiCDTAFile.Length != existingFileInfo.Length)
                    {
                        var msg = "Dataset " + dataPkgJob.Dataset +
                                  " has multiple jobs in this data package, and those jobs used different _dta.txt files; this is not supported";
                        LogError(msg + ": file size mismatch of " + fiCDTAFile.Length + " for job " + dataPkgJob.Job + " vs " + existingFileInfo.Length +
                                 " for job " + existingFileInfo.JobInfo.Job);
                        m_message = msg;
                        return false;
                    }

                    if (md5Hash != existingFileInfo.MD5Hash)
                    {
                        var msg = "Dataset " + dataPkgJob.Dataset +
                                  " has multiple jobs in this data package, and those jobs used different _dta.txt files; this is not supported";
                        LogError(msg + ": MD5 hash mismatch of " + md5Hash + " for job " + dataPkgJob.Job + " vs. " + existingFileInfo.MD5Hash +
                                 " for job " + existingFileInfo.JobInfo.Job);
                        m_message = msg;
                        return false;
                    }

                    // The files match; no point in making a new .mgf file
                    return true;
                }

                var filename = CheckFilenameCase(fiCDTAFile, dataPkgJob.Dataset);

                var oFileInfo = new clsPXFileInfoBase(filename, dataPkgJob)
                {
                    // File ID doesn't matter; just use 0
                    FileID = 0,
                    Length = fiCDTAFile.Length,
                    MD5Hash = md5Hash
                };

                mCDTAFileStats.Add(fiCDTAFile.Name, oFileInfo);

                if (!mDTAtoMGF.ProcessFile(fiCDTAFile.FullName))
                {
                    var msg = "Error converting " + fiCDTAFile.Name + " to a .mgf file for job " + dataPkgJob.Job;
                    LogError(msg + ": " + mDTAtoMGF.GetErrorMessage());
                    m_message = msg;
                    return false;
                }

                // Delete the _dta.txt file
                try
                {
                    fiCDTAFile.Delete();
                }
                catch (Exception)
                {
                    // Ignore errors here
                }

                PRISM.clsProgRunner.GarbageCollectNow();

                var fiNewMGFFile = new FileInfo(Path.Combine(m_WorkDir, dataPkgJob.Dataset + DOT_MGF));

                if (!fiNewMGFFile.Exists)
                {
                    // MGF file was not created
                    var msg = "A .mgf file was not created for the _dta.txt file for job " + dataPkgJob.Job;
                    m_message = msg;
                    LogError(msg + ": " + mDTAtoMGF.GetErrorMessage());
                    return false;
                }

                mgfFilePath = fiNewMGFFile.FullName;
            }
            catch (Exception ex)
            {
                LogError("Exception in ConvertCDTAToMGF", ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Copy failed results to the archive folder
        /// </summary>
        public override void CopyFailedResultsToArchiveFolder()
        {

            // Make sure the PRIDEConverter console output file is retained
            m_jobParams.RemoveResultFileToSkip(PRIDEConverter_CONSOLE_OUTPUT);

            // Skip the .mgf files; no need to put them in the FailedResults folder
            m_jobParams.AddResultFileExtensionToSkip(DOT_MGF);

            base.CopyFailedResultsToArchiveFolder();
        }

        /// <summary>
        /// Counts the number of items of type eFileType in mPxResultFiles
        /// </summary>
        /// <param name="eFileType"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private int CountResultFilesByType(clsPXFileInfoBase.ePXFileType eFileType)
        {
            var fileCount = (from item in mPxResultFiles where item.Value.PXFileType == eFileType select item).Count();

            return fileCount;
        }

        /// <summary>
        /// Creates (or retrieves) the .mzXML file for this dataset if it does not exist in the working directory
        /// Utilizes dataset info stored in several packed job parameters
        /// Newly created .mzXML files will be copied to the MSXML_Cache folder
        /// </summary>
        /// <returns>True if the file exists or was created</returns>
        /// <remarks></remarks>
        private bool CreateMzXMLFileIfMissing(string dataset, clsAnalysisResults analysisResults,
            IReadOnlyDictionary<string, string> datasetRawFilePaths)
        {
            try
            {
                // Look in m_WorkDir for the .mzXML file for this dataset
                var fiMzXmlFilePathLocal = new FileInfo(Path.Combine(m_WorkDir, dataset + clsAnalysisResources.DOT_MZXML_EXTENSION));

                if (fiMzXmlFilePathLocal.Exists)
                {
                    if (!mPreviousDatasetFilesToDelete.Contains(fiMzXmlFilePathLocal.FullName))
                    {
                        AddToListIfNew(mPreviousDatasetFilesToDelete, fiMzXmlFilePathLocal.FullName);
                    }
                    return true;
                }

                // .mzXML file not found
                // Look for a StoragePathInfo file
                var mzXmlStoragePathFile = fiMzXmlFilePathLocal.FullName + clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX;

                string destPath;
                bool success;
                if (File.Exists(mzXmlStoragePathFile))
                {
                    success = RetrieveStoragePathInfoTargetFile(mzXmlStoragePathFile, analysisResults, out destPath);
                    if (success)
                    {
                        AddToListIfNew(mPreviousDatasetFilesToDelete, destPath);
                        return true;
                    }
                }

                // Need to create the .mzXML file

                var datasetYearQuarterByDataset =
                    ExtractPackedJobParameterDictionary(clsAnalysisResourcesPRIDEConverter.JOB_PARAM_DICTIONARY_DATASET_STORAGE_YEAR_QUARTER);

                if (!datasetRawFilePaths.ContainsKey(dataset))
                {
                    LogError("Dataset " + dataset + " not found in job parameter " +
                        clsAnalysisResources.JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS +
                        "; unable to create the missing .mzXML file");
                    return false;
                }

                m_jobParams.AddResultFileToSkip("MSConvert_ConsoleOutput.txt");

                mMSXmlCreator = new AnalysisManagerMsXmlGenPlugIn.clsMSXMLCreator(mMSXmlGeneratorAppPath, m_WorkDir, m_Dataset, m_DebugLevel,
                    m_jobParams);
                RegisterEvents(mMSXmlCreator);
                mMSXmlCreator.LoopWaiting += mMSXmlCreator_LoopWaiting;

                mMSXmlCreator.UpdateDatasetName(dataset);

                // Make sure the dataset file is present in the working directory
                // Copy it locally if necessary

                var datasetFilePathRemote = datasetRawFilePaths[dataset];
                if (string.IsNullOrWhiteSpace(datasetFilePathRemote))
                {
                    LogError("Dataset " + dataset + " has an empty value for the instrument file path in " +
                        clsAnalysisResources.JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS +
                        "; unable to create the missing .mzXML file");
                    return false;
                }
                var datasetFileIsAFolder = Directory.Exists(datasetFilePathRemote);

                var datasetFilePathLocal = Path.Combine(m_WorkDir, Path.GetFileName(datasetFilePathRemote));

                if (datasetFileIsAFolder)
                {
                    // Confirm that the dataset directory exists in the working directory

                    if (!Directory.Exists(datasetFilePathLocal))
                    {
                        // Directory not found; look for a storage path info file
                        if (File.Exists(datasetFilePathLocal + clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX))
                        {
                            RetrieveStoragePathInfoTargetFile(datasetFilePathLocal + clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX,
                                analysisResults, IsFolder: true, destPath: out destPath);
                        }
                        else
                        {
                            // Copy the dataset directory locally
                            analysisResults.CopyDirectory(datasetFilePathRemote, datasetFilePathLocal, overwrite: true);
                        }
                    }
                }
                else
                {
                    // Confirm that the dataset file exists in the working directory
                    if (!File.Exists(datasetFilePathLocal))
                    {
                        // File not found; Look for a storage path info file
                        if (File.Exists(datasetFilePathLocal + clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX))
                        {
                            RetrieveStoragePathInfoTargetFile(datasetFilePathLocal + clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX,
                                analysisResults, out destPath);
                            AddToListIfNew(mPreviousDatasetFilesToDelete, destPath);
                        }
                        else
                        {
                            // Copy the dataset file locally
                            analysisResults.CopyFileWithRetry(datasetFilePathRemote, datasetFilePathLocal, overwrite: true);
                            AddToListIfNew(mPreviousDatasetFilesToDelete, datasetFilePathLocal);
                        }
                    }
                    m_jobParams.AddResultFileToSkip(Path.GetFileName(datasetFilePathLocal));
                }

                success = mMSXmlCreator.CreateMZXMLFile();

                if (!success && string.IsNullOrEmpty(m_message))
                {
                    m_message = mMSXmlCreator.ErrorMessage;
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "Unknown error creating the mzXML file for dataset " + dataset;
                    }
                    else if (!m_message.Contains(dataset))
                    {
                        m_message += "; dataset " + dataset;
                    }
                    LogError(m_message);
                }

                if (!success)
                    return false;

                fiMzXmlFilePathLocal.Refresh();
                if (fiMzXmlFilePathLocal.Exists)
                {
                    AddToListIfNew(mPreviousDatasetFilesToDelete, fiMzXmlFilePathLocal.FullName);
                }
                else
                {
                    LogError("MSXmlCreator did not create the .mzXML file for dataset " + dataset);
                    return false;
                }

                // Copy the .mzXML file to the cache

                var msXmlGeneratorName = Path.GetFileNameWithoutExtension(mMSXmlGeneratorAppPath);
                if (!datasetYearQuarterByDataset.TryGetValue(dataset, out var datasetYearQuarter))
                {
                    datasetYearQuarter = string.Empty;
                }

                CopyMzXMLFileToServerCache(fiMzXmlFilePathLocal.FullName, datasetYearQuarter, msXmlGeneratorName, purgeOldFilesIfNeeded: true);

                m_jobParams.AddResultFileToSkip(Path.GetFileName(fiMzXmlFilePathLocal.FullName + clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX));

                PRISM.clsProgRunner.GarbageCollectNow();

                try
                {
                    if (datasetFileIsAFolder)
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
                        if (File.Exists(datasetFilePathLocal))
                        {
                            File.Delete(datasetFilePathLocal);
                        }
                    }
                }
                catch (Exception)
                {
                    // Ignore errors here
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception in CreateMzXMLFileIfMissing", ex);
                return false;
            }

        }

        private string CreatePseudoMSGFFileUsingPHRPReader(int job, string dataset, udtFilterThresholdsType udtFilterThresholds,
            IDictionary<string, List<udtPseudoMSGFDataType>> pseudoMSGFData)
        {
            const int MSGF_SPECEVALUE_NOTDEFINED = 10;
            const int PVALUE_NOTDEFINED = 10;

            string pseudoMsgfFilePath;

            var fdrValuesArePresent = false;
            var pepFDRValuesArePresent = false;
            var msgfValuesArePresent = false;

            try
            {
                if (!mDataPackagePeptideHitJobs.TryGetValue(job, out var dataPkgJob))
                {
                    LogError("Job " + job + " not found in mDataPackagePeptideHitJobs; this is unexpected");
                    return string.Empty;
                }

                if (pseudoMSGFData.Count > 0)
                {
                    pseudoMSGFData.Clear();
                }

                // The .MSGF file can only contain one match for each scan number
                // If it includes multiple matches, PRIDE Converter crashes when reading the .mzXML file
                // Furthermore, the .msgf-report.xml file cannot have extra entries that are not in the .msgf file
                // Thus, only keep the best-scoring match for each spectrum
                //
                // The keys in each of bestMatchByScan and bestMatchByScanScoreValues are scan numbers
                // The value for bestMatchByScan is a KeyValue pair where the key is the score for this match
                var bestMatchByScan = new Dictionary<int, KeyValuePair<double, string>>();
                var bestMatchByScanScoreValues = new Dictionary<int, udtPseudoMSGFDataType>();

                var mzXMLFilename = dataset + ".mzXML";

                // Determine the correct capitalization for the mzXML file
                var diWorkdir = new DirectoryInfo(m_WorkDir);
                var fiFiles = diWorkdir.GetFiles(mzXMLFilename);

                if (fiFiles.Length > 0)
                {
                    mzXMLFilename = fiFiles[0].Name;
                }
                // else
                // mzXML file not found; don't worry about this right now (it's possible that CreateMSGFReportFilesOnly = True)

                var synopsisFileName = clsPHRPReader.GetPHRPSynopsisFileName(dataPkgJob.PeptideHitResultType, dataPkgJob.Dataset);

                var synopsisFilePath = Path.Combine(m_WorkDir, synopsisFileName);

                if (!File.Exists(synopsisFilePath))
                {
                    var synopsisFilePathAlt = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(synopsisFilePath, "Dataset_msgfdb.txt");
                    if (File.Exists(synopsisFilePathAlt))
                    {
                        synopsisFilePath = synopsisFilePathAlt;
                    }
                }

                // Check whether PHRP files with a prefix of "Job12345_" exist
                // This prefix is added by RetrieveDataPackagePeptideHitJobPHRPFiles if multiple peptide_hit jobs are included for the same dataset
                var synopsisFilePathWithJob = Path.Combine(m_WorkDir, "Job" + dataPkgJob.Job + "_" + synopsisFileName);

                if (File.Exists(synopsisFilePathWithJob))
                {
                    synopsisFilePath = string.Copy(synopsisFilePathWithJob);
                }
                else if (!File.Exists(synopsisFilePath))
                {
                    var synopsisFilePathAlt = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(synopsisFilePathWithJob, "Dataset_msgfdb.txt");
                    if (File.Exists(synopsisFilePathAlt))
                    {
                        synopsisFilePath = synopsisFilePathAlt;
                    }
                }

                using (var reader = new clsPHRPReader(synopsisFilePath, dataPkgJob.PeptideHitResultType, true, true))
                {
                    RegisterEvents(reader);

                    reader.SkipDuplicatePSMs = false;

                    // Read the data, filtering on either PepFDR or FDR if defined, or MSGF_SpecEValue if PepFDR and/or FDR are not available

                    while (reader.MoveNext())
                    {
                        var validPSM = true;
                        var thresholdChecked = false;

                        var fdr = -1.0;
                        var pepFDR = -1.0;
                        var pValue = (double)PVALUE_NOTDEFINED;
                        var scoreForCurrentMatch = 100.0;

                        // Determine MSGFSpecEValue; store 10 if we don't find a valid number
                        if (!double.TryParse(reader.CurrentPSM.MSGFSpecEValue, out var msgfSpecEValue))
                        {
                            msgfSpecEValue = MSGF_SPECEVALUE_NOTDEFINED;
                        }

                        switch (dataPkgJob.PeptideHitResultType)
                        {
                            case clsPHRPReader.ePeptideHitResultType.Sequest:
                                if (msgfSpecEValue < MSGF_SPECEVALUE_NOTDEFINED)
                                {
                                    pValue = ComputeApproximateEValue(msgfSpecEValue);
                                    scoreForCurrentMatch = msgfSpecEValue;
                                    msgfValuesArePresent = true;
                                }
                                else
                                {
                                    if (msgfValuesArePresent)
                                    {
                                        // Skip this result; it had a score value too low to be processed with MSGF
                                        pValue = 1;
                                        validPSM = false;
                                    }
                                    else
                                    {
                                        pValue = 0.025;
                                        // Note: storing 1000-XCorr so that lower values will be considered higher confidence
                                        scoreForCurrentMatch = 1000 - (reader.CurrentPSM.GetScoreDbl(clsPHRPParserSequest.DATA_COLUMN_XCorr, 1));
                                    }
                                }
                                break;

                            case clsPHRPReader.ePeptideHitResultType.XTandem:
                                if (msgfSpecEValue < MSGF_SPECEVALUE_NOTDEFINED)
                                {
                                    pValue = ComputeApproximateEValue(msgfSpecEValue);
                                    scoreForCurrentMatch = msgfSpecEValue;
                                    msgfValuesArePresent = true;
                                }
                                else
                                {
                                    if (msgfValuesArePresent)
                                    {
                                        // Skip this result; it had a score value too low to be processed with MSGF
                                        pValue = 1;
                                        validPSM = false;
                                    }
                                    else
                                    {
                                        pValue = 0.025;
                                        scoreForCurrentMatch = 1000 + reader.CurrentPSM.GetScoreDbl(clsPHRPParserXTandem.DATA_COLUMN_Peptide_Expectation_Value_LogE, 1);
                                    }
                                }
                                break;

                            case clsPHRPReader.ePeptideHitResultType.Inspect:
                                pValue = reader.CurrentPSM.GetScoreDbl(clsPHRPParserInspect.DATA_COLUMN_PValue, PVALUE_NOTDEFINED);

                                if (msgfSpecEValue < MSGF_SPECEVALUE_NOTDEFINED)
                                {
                                    scoreForCurrentMatch = msgfSpecEValue;
                                }
                                else
                                {
                                    if (msgfValuesArePresent)
                                    {
                                        // Skip this result; it had a score value too low to be processed with MSGF
                                        pValue = 1;
                                        validPSM = false;
                                    }
                                    else
                                    {
                                        // Note: storing 1000-TotalPRMScore so that lower values will be considered higher confidence
                                        scoreForCurrentMatch = 1000 - (reader.CurrentPSM.GetScoreDbl(clsPHRPParserInspect.DATA_COLUMN_TotalPRMScore, 1));
                                    }
                                }
                                break;

                            case clsPHRPReader.ePeptideHitResultType.MSGFDB:
                                fdr = reader.CurrentPSM.GetScoreDbl(clsPHRPParserMSGFDB.DATA_COLUMN_FDR, -1);
                                if (fdr > -1)
                                {
                                    fdrValuesArePresent = true;
                                }

                                pepFDR = reader.CurrentPSM.GetScoreDbl(clsPHRPParserMSGFDB.DATA_COLUMN_PepFDR, -1);
                                if (pepFDR > -1)
                                {
                                    pepFDRValuesArePresent = true;
                                }

                                pValue = reader.CurrentPSM.GetScoreDbl(clsPHRPParserMSGFDB.DATA_COLUMN_PValue, PVALUE_NOTDEFINED);
                                scoreForCurrentMatch = msgfSpecEValue;
                                break;
                        }

                        if (udtFilterThresholds.UseMSGFSpecEValue)
                        {
                            if (msgfSpecEValue > udtFilterThresholds.MSGFSpecEValueThreshold)
                            {
                                validPSM = false;
                            }
                            thresholdChecked = true;

                            if (!mFilterThresholdsUsed.UseMSGFSpecEValue)
                            {
                                mFilterThresholdsUsed.UseMSGFSpecEValue = true;
                                mFilterThresholdsUsed.MSGFSpecEValueThreshold = udtFilterThresholds.MSGFSpecEValueThreshold;
                            }
                        }

                        if (pepFDRValuesArePresent && udtFilterThresholds.UsePepFDRThreshold)
                        {
                            // Typically only MSGFDB results will have PepFDR values
                            if (pepFDR > udtFilterThresholds.PepFDRThreshold)
                            {
                                validPSM = false;
                            }
                            thresholdChecked = true;

                            if (!mFilterThresholdsUsed.UsePepFDRThreshold)
                            {
                                mFilterThresholdsUsed.UsePepFDRThreshold = true;
                                mFilterThresholdsUsed.PepFDRThreshold = udtFilterThresholds.PepFDRThreshold;
                            }
                        }

                        if (fdrValuesArePresent && udtFilterThresholds.UseFDRThreshold)
                        {
                            // Typically only MSGFDB results will have FDR values
                            if (fdr > udtFilterThresholds.FDRThreshold)
                            {
                                validPSM = false;
                            }
                            thresholdChecked = true;

                            if (!mFilterThresholdsUsed.UseFDRThreshold)
                            {
                                mFilterThresholdsUsed.UseFDRThreshold = true;
                                mFilterThresholdsUsed.FDRThreshold = udtFilterThresholds.FDRThreshold;
                            }
                        }

                        if (validPSM && !thresholdChecked)
                        {
                            // Switch to filtering on MSGFSpecEValueThreshold instead of on FDR or PepFDR
                            if (msgfSpecEValue < MSGF_SPECEVALUE_NOTDEFINED && udtFilterThresholds.MSGFSpecEValueThreshold < 0.0001)
                            {
                                if (msgfSpecEValue > udtFilterThresholds.MSGFSpecEValueThreshold)
                                {
                                    validPSM = false;
                                }

                                if (!mFilterThresholdsUsed.UseMSGFSpecEValue)
                                {
                                    mFilterThresholdsUsed.UseMSGFSpecEValue = true;
                                    mFilterThresholdsUsed.MSGFSpecEValueThreshold = udtFilterThresholds.MSGFSpecEValueThreshold;
                                }
                            }
                        }

                        if (validPSM)
                        {
                            // Filter on P-value
                            if (pValue >= udtFilterThresholds.PValueThreshold)
                            {
                                validPSM = false;
                            }
                        }

                        if (validPSM)
                        {
                            // Determine the protein index in mCachedProteins


                            if (!mCachedProteins.TryGetValue(reader.CurrentPSM.ProteinFirst, out var indexAndSequence))
                            {
                                // Protein not found in mCachedProteins
                                // If the search engine is MSGFDB and the protein name starts with REV_ or XXX_ then skip this protein since it's a decoy result
                                // Otherwise, add the protein to mCachedProteins and mCachedProteinPSMCounts, though we won't know its sequence

                                var proteinUCase = reader.CurrentPSM.ProteinFirst.ToUpper();

                                if (dataPkgJob.PeptideHitResultType == clsPHRPReader.ePeptideHitResultType.MSGFDB)
                                {
                                    if (proteinUCase.StartsWith("REV_") || proteinUCase.StartsWith("XXX_"))
                                    {
                                        validPSM = false;
                                    }
                                }
                                else
                                {
                                    if (proteinUCase.StartsWith("REVERSED_") || proteinUCase.StartsWith("SCRAMBLED_") ||
                                        proteinUCase.StartsWith("XXX."))
                                    {
                                        validPSM = false;
                                    }
                                }

                                if (validPSM)
                                {
                                    indexAndSequence = new KeyValuePair<int, string>(mCachedProteins.Count, string.Empty);
                                    mCachedProteinPSMCounts.Add(indexAndSequence.Key, 0);
                                    mCachedProteins.Add(reader.CurrentPSM.ProteinFirst, indexAndSequence);
                                }
                            }
                        }

                        if (!validPSM)
                            continue;

                        // These fields are used to hold different scores depending on the search engine
                        var totalPRMScore = "0";
                        var pValueFormatted = "0";
                        var deltaScore = "0";
                        var deltaScoreOther = "0";

                        switch (dataPkgJob.PeptideHitResultType)
                        {
                            case clsPHRPReader.ePeptideHitResultType.Sequest:
                                totalPRMScore = reader.CurrentPSM.GetScore(clsPHRPParserSequest.DATA_COLUMN_Sp);
                                pValueFormatted = pValue.ToString("0.00");
                                deltaScore = reader.CurrentPSM.GetScore(clsPHRPParserSequest.DATA_COLUMN_DelCn);
                                deltaScoreOther = reader.CurrentPSM.GetScore(clsPHRPParserSequest.DATA_COLUMN_DelCn2);
                                break;

                            case clsPHRPReader.ePeptideHitResultType.XTandem:
                                totalPRMScore = reader.CurrentPSM.GetScore(clsPHRPParserXTandem.DATA_COLUMN_Peptide_Hyperscore);
                                pValueFormatted = pValue.ToString("0.00");
                                deltaScore = reader.CurrentPSM.GetScore(clsPHRPParserXTandem.DATA_COLUMN_DeltaCn2);
                                break;

                            case clsPHRPReader.ePeptideHitResultType.Inspect:
                                totalPRMScore = reader.CurrentPSM.GetScore(clsPHRPParserInspect.DATA_COLUMN_TotalPRMScore);
                                pValueFormatted = reader.CurrentPSM.GetScore(clsPHRPParserInspect.DATA_COLUMN_PValue);
                                deltaScore = reader.CurrentPSM.GetScore(clsPHRPParserInspect.DATA_COLUMN_DeltaScore);
                                deltaScoreOther = reader.CurrentPSM.GetScore(clsPHRPParserInspect.DATA_COLUMN_DeltaScoreOther);
                                break;

                            case clsPHRPReader.ePeptideHitResultType.MSGFDB:
                                totalPRMScore = reader.CurrentPSM.GetScore(clsPHRPParserMSGFDB.DATA_COLUMN_DeNovoScore);
                                pValueFormatted = reader.CurrentPSM.GetScore(clsPHRPParserMSGFDB.DATA_COLUMN_PValue);
                                break;
                        }

                        // Construct the text that we would write to the pseudo MSGF file
                        var msgfText =
                            mzXMLFilename + "\t" +
                            reader.CurrentPSM.ScanNumber + "\t" +
                            reader.CurrentPSM.Peptide + "\t" +
                            reader.CurrentPSM.ProteinFirst + "\t" +
                            reader.CurrentPSM.Charge + "\t" +
                            reader.CurrentPSM.MSGFSpecEValue + "\t" +
                            reader.CurrentPSM.PeptideCleanSequence.Length + "\t" +
                            totalPRMScore + "\t" + "0\t" + "0\t" + "0\t" + "0\t" +
                            reader.CurrentPSM.NumTrypticTerminii + "\t" +
                            pValueFormatted + "\t" + "0\t" +
                            deltaScore + "\t" +
                            deltaScoreOther + "\t" +
                            reader.CurrentPSM.ResultID + "\t" + "0\t" + "0\t" +
                            reader.CurrentPSM.MSGFSpecEValue;

                        // Add or update bestMatchByScan and bestMatchByScanScoreValues
                        bool newScanNumber;

                        if (bestMatchByScan.TryGetValue(reader.CurrentPSM.ScanNumber, out var bestMatchForScan))
                        {
                            if (scoreForCurrentMatch >= bestMatchForScan.Key)
                            {
                                // Skip this result since it has a lower score than the match already stored in bestMatchByScan
                                validPSM = false;
                            }
                            else
                            {
                                // Update bestMatchByScan
                                bestMatchByScan[reader.CurrentPSM.ScanNumber] = new KeyValuePair<double, string>(scoreForCurrentMatch, msgfText);
                                validPSM = true;
                            }
                            newScanNumber = false;
                        }
                        else
                        {
                            // Scan not yet present in bestMatchByScan; add it
                            bestMatchForScan = new KeyValuePair<double, string>(scoreForCurrentMatch, msgfText);
                            bestMatchByScan.Add(reader.CurrentPSM.ScanNumber, bestMatchForScan);
                            validPSM = true;
                            newScanNumber = true;
                        }

                        if (!validPSM)
                            continue;

                        if (!clsPeptideCleavageStateCalculator.SplitPrefixAndSuffixFromSequence(
                            reader.CurrentPSM.Peptide, out _, out var prefix, out var suffix))
                        {
                            prefix = string.Empty;
                            suffix = string.Empty;
                        }

                        var udtPseudoMSGFData = new udtPseudoMSGFDataType
                        {
                            ResultID = reader.CurrentPSM.ResultID,
                            Peptide = string.Copy(reader.CurrentPSM.Peptide),
                            CleanSequence = string.Copy(reader.CurrentPSM.PeptideCleanSequence),
                            PrefixResidue = string.Copy(prefix),
                            SuffixResidue = string.Copy(suffix),
                            ScanNumber = reader.CurrentPSM.ScanNumber,
                            ChargeState = reader.CurrentPSM.Charge,
                            PValue = string.Copy(pValueFormatted),
                            MQScore = string.Copy(reader.CurrentPSM.MSGFSpecEValue),
                            TotalPRMScore = string.Copy(totalPRMScore),
                            NTT = reader.CurrentPSM.NumTrypticTerminii,
                            MSGFSpecEValue = string.Copy(reader.CurrentPSM.MSGFSpecEValue),
                            DeltaScore = string.Copy(deltaScore),
                            DeltaScoreOther = string.Copy(deltaScoreOther),
                            Protein = reader.CurrentPSM.ProteinFirst
                        };

                        if (newScanNumber)
                        {
                            bestMatchByScanScoreValues.Add(reader.CurrentPSM.ScanNumber, udtPseudoMSGFData);
                        }
                        else
                        {
                            bestMatchByScanScoreValues[reader.CurrentPSM.ScanNumber] = udtPseudoMSGFData;
                        }
                    }
                }

                if (JobFileRenameRequired(job))
                {
                    pseudoMsgfFilePath = Path.Combine(m_WorkDir, dataPkgJob.Dataset + "_Job" + dataPkgJob.Job + FILE_EXTENSION_PSEUDO_MSGF);
                }
                else
                {
                    pseudoMsgfFilePath = Path.Combine(m_WorkDir, dataPkgJob.Dataset + FILE_EXTENSION_PSEUDO_MSGF);
                }

                using (var swMSGFFile = new StreamWriter(new FileStream(pseudoMsgfFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    // Write the header line
                    swMSGFFile.WriteLine("#SpectrumFile\t" + "Scan#\t" + "Annotation\t" + "Protein\t" + "Charge\t" + "MQScore\t" + "Length\t" +
                                         "TotalPRMScore\t" + "MedianPRMScore\t" + "FractionY\t" + "FractionB\t" + "Intensity\t" + "NTT\t" +
                                         "p-value\t" + "F-Score\t" + "DeltaScore\t" + "DeltaScoreOther\t" + "RecordNumber\t" + "DBFilePos\t" +
                                         "SpecFilePos\t" + "SpecProb");

                    // Write out the filter-passing matches to the pseudo MSGF text file
                    foreach (var item in bestMatchByScan)
                    {
                        swMSGFFile.WriteLine(item.Value.Value);
                    }
                }

                // Store the filter-passing matches in pseudoMSGFData

                foreach (var item in bestMatchByScanScoreValues)
                {
                    if (pseudoMSGFData.TryGetValue(item.Value.Protein, out var matchesForProtein))
                    {
                        matchesForProtein.Add(item.Value);
                    }
                    else
                    {
                        matchesForProtein = new List<udtPseudoMSGFDataType> {
                            item.Value
                        };
                        pseudoMSGFData.Add(item.Value.Protein, matchesForProtein);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in CreatePseudoMSGFFileUsingPHRPReader", ex);
                return string.Empty;
            }

            return pseudoMsgfFilePath;
        }

        /// <summary>
        /// Create the .msgf-report.xml file
        /// </summary>
        /// <param name="job"></param>
        /// <param name="dataset"></param>
        /// <param name="udtFilterThresholds"></param>
        /// <param name="prideReportXMLFilePath">Output parameter: the full path of the newly created .msgf-report.xml file</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        private bool CreateMSGFReportFile(int job, string dataset, udtFilterThresholdsType udtFilterThresholds,
            out string prideReportXMLFilePath)
        {
            var localOrgDBFolder = m_mgrParams.GetParam("orgdbdir");

            var pseudoMSGFData = new Dictionary<string, List<udtPseudoMSGFDataType>>();

            prideReportXMLFilePath = string.Empty;

            try
            {
                var templateFileName = clsAnalysisResourcesPRIDEConverter.GetMSGFReportTemplateFilename(m_jobParams, WarnIfJobParamMissing: false);

                var orgDBNameGenerated = m_jobParams.GetJobParameter("PeptideSearch",
                                                                           clsAnalysisResourcesPRIDEConverter.GetGeneratedFastaParamNameForJob(job), string.Empty);
                if (string.IsNullOrEmpty(orgDBNameGenerated))
                {
                    LogError("Job parameter " + clsAnalysisResourcesPRIDEConverter.GetGeneratedFastaParamNameForJob(job) +
                             " was not found in CreateMSGFReportFile; unable to continue");
                    return false;
                }

                if (!mDataPackagePeptideHitJobs.TryGetValue(job, out var dataPkgJob))
                {
                    LogError("Job " + job + " not found in mDataPackagePeptideHitJobs; unable to continue");
                    return false;
                }

                string proteinCollectionListOrFasta;
                if (!string.IsNullOrEmpty(dataPkgJob.ProteinCollectionList) && dataPkgJob.ProteinCollectionList != "na")
                {
                    proteinCollectionListOrFasta = dataPkgJob.ProteinCollectionList;
                }
                else
                {
                    proteinCollectionListOrFasta = dataPkgJob.LegacyFastaFileName;
                }

                if (mCachedOrgDBName != orgDBNameGenerated)
                {
                    // Need to read the proteins from the fasta file

                    mCachedProteins.Clear();
                    mCachedProteinPSMCounts.Clear();

                    var fastaFilePath = Path.Combine(localOrgDBFolder, orgDBNameGenerated);
                    var fastaFileReader = new ProteinFileReader.FastaFileReader();

                    if (!fastaFileReader.OpenFile(fastaFilePath))
                    {
                        var msg = "Error opening fasta file " + orgDBNameGenerated + "; fastaFileReader.OpenFile() returned false";
                        LogError(msg + "; see " + localOrgDBFolder);
                        m_message = msg;
                        return false;
                    }

                    LogDebug("Reading proteins from " + fastaFilePath, 10);

                    while (fastaFileReader.ReadNextProteinEntry())
                    {
                        if (!mCachedProteins.ContainsKey(fastaFileReader.ProteinName))
                        {
                            var indexAndSequence = new KeyValuePair<int, string>(mCachedProteins.Count, fastaFileReader.ProteinSequence);

                            try
                            {
                                mCachedProteins.Add(fastaFileReader.ProteinName, indexAndSequence);
                            }
                            catch (Exception ex)
                            {
                                throw new Exception("Dictionary error adding to mCachedProteins", ex);
                            }

                            try
                            {
                                mCachedProteinPSMCounts.Add(indexAndSequence.Key, 0);
                            }
                            catch (Exception ex)
                            {
                                throw new Exception("Dictionary error adding to mCachedProteinPSMCounts", ex);
                            }
                        }
                    }
                    fastaFileReader.CloseFile();

                    mCachedOrgDBName = string.Copy(orgDBNameGenerated);
                }
                else
                {
                    // Reset the counts in mCachedProteinPSMCounts
                    for (var i = 0; i <= mCachedProteinPSMCounts.Count; i++)
                    {
                        mCachedProteinPSMCounts[i] = 0;
                    }
                }

                pseudoMSGFData.Clear();

                var pseudoMsgfFilePath = CreatePseudoMSGFFileUsingPHRPReader(job, dataset, udtFilterThresholds, pseudoMSGFData);

                if (string.IsNullOrEmpty(pseudoMsgfFilePath))
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        LogError("Pseudo Msgf file not created for job " + job + ", dataset " + dataset);
                    }
                    return false;
                }

                AddToListIfNew(mPreviousDatasetFilesToDelete, pseudoMsgfFilePath);

                if (!mCreateMSGFReportFilesOnly)
                {
                    prideReportXMLFilePath = CreateMSGFReportXMLFile(templateFileName, dataPkgJob, pseudoMsgfFilePath, pseudoMSGFData,
                        orgDBNameGenerated, proteinCollectionListOrFasta, udtFilterThresholds);

                    if (string.IsNullOrEmpty(prideReportXMLFilePath))
                    {
                        if (string.IsNullOrEmpty(m_message))
                        {
                            LogError("Pride report XML file not created for job " + job + ", dataset " + dataset);
                        }
                        return false;
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception in CreateMSGFReportFile for job " + job + ", dataset " + dataset, ex);
                return false;
            }


        }

        private string CreateMSGFReportXMLFile(string templateFileName, clsDataPackageJobInfo dataPkgJob, string pseudoMsgfFilePath,
            IReadOnlyDictionary<string, List<udtPseudoMSGFDataType>> pseudoMSGFData, string orgDBNameGenerated,
            string proteinCollectionListOrFasta, udtFilterThresholdsType udtFilterThresholds)
        {
            string prideReportXMLFilePath;

            var insideMzDataDescription = false;
            var instrumentDetailsAutoDefined = false;

            var attributeOverride = new Dictionary<string, string>();

            var eFileLocation = eMSGFReportXMLFileLocation.Header;
            var recentElements = new Queue<string>();

            try
            {
                var elementCloseDepths = new Stack<int>();

                // Open templateFileName and parse it to create a new XML file
                // Use a forward-only XML reader, copying some elements verbatim and customizing others
                // When we reach <Identifications>, we write out the data that was cached from pseudoMsgfFilePath
                //    Must write out data by protein

                // Next, append the protein sequences in mCachedProteinPSMCounts to the <Fasta></Fasta> section

                // Finally, write the remaining sections
                // <PTMs>
                // <DatabaseMappings>
                // <ConfigurationOptions>

                prideReportXMLFilePath = pseudoMsgfFilePath + "-report.xml";

                using (var writer = new XmlTextWriter(
                    new FileStream(prideReportXMLFilePath, FileMode.Create, FileAccess.Write, FileShare.Read),
                    new UTF8Encoding(false)))
                using (var xmlReader = new XmlTextReader(
                    new FileStream(Path.Combine(m_WorkDir, templateFileName), FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    writer.Formatting = Formatting.Indented;
                    writer.Indentation = 4;

                    writer.WriteStartDocument();

                    while (xmlReader.Read())
                    {
                        switch (xmlReader.NodeType)
                        {
                            case XmlNodeType.Whitespace:
                                // Skip whitespace since the writer should be auto-formatting things
                                // writer.WriteWhitespace(xmlReader.Value)
                                break;

                            case XmlNodeType.Comment:
                                writer.WriteComment(xmlReader.Value);
                                break;

                            case XmlNodeType.Element:
                                // Start element

                                if (recentElements.Count > 10)
                                    recentElements.Dequeue();
                                recentElements.Enqueue("Element " + xmlReader.Name);

                                while (elementCloseDepths.Count > 0 && elementCloseDepths.Peek() > xmlReader.Depth)
                                {
                                    elementCloseDepths.Pop();

                                    writer.WriteEndElement();
                                }

                                eFileLocation = UpdateMSGFReportXMLFileLocation(eFileLocation, xmlReader.Name, insideMzDataDescription);

                                var skipNode = false;
                                attributeOverride.Clear();

                                switch (xmlReader.Name)
                                {
                                    case "sourceFilePath":
                                        // Update this element's value to contain pseudoMsgfFilePath
                                        writer.WriteElementString("sourceFilePath", pseudoMsgfFilePath);
                                        skipNode = true;
                                        break;

                                    case "timeCreated":
                                        // Write out the current date/time in this format: 2012-11-06T16:04:44Z
                                        writer.WriteElementString("timeCreated", DateTime.Now.ToUniversalTime().ToString("s") + "Z");
                                        skipNode = true;
                                        break;

                                    case "MzDataDescription":
                                        insideMzDataDescription = true;
                                        break;

                                    case "sampleName":
                                        if (eFileLocation == eMSGFReportXMLFileLocation.MzDataAdmin)
                                        {
                                            // Write out the current job's Experiment Name
                                            writer.WriteElementString("sampleName", dataPkgJob.Experiment);
                                            skipNode = true;
                                        }
                                        break;

                                    case "sampleDescription":
                                        if (eFileLocation == eMSGFReportXMLFileLocation.MzDataAdmin)
                                        {
                                            // Override the comment attribute for this node
                                            string commentOverride;

                                            if (!string.IsNullOrWhiteSpace(dataPkgJob.Experiment_Reason))
                                            {
                                                commentOverride = dataPkgJob.Experiment_Reason.TrimEnd();

                                                if (!string.IsNullOrWhiteSpace(dataPkgJob.Experiment_Comment))
                                                {
                                                    if (commentOverride.EndsWith("."))
                                                    {
                                                        commentOverride += " " + dataPkgJob.Experiment_Comment;
                                                    }
                                                    else
                                                    {
                                                        commentOverride += ". " + dataPkgJob.Experiment_Comment;
                                                    }
                                                }
                                            }
                                            else
                                            {
                                                commentOverride = dataPkgJob.Experiment_Comment;
                                            }

                                            attributeOverride.Add("comment", commentOverride);
                                        }
                                        break;

                                    case "sourceFile":
                                        if (eFileLocation == eMSGFReportXMLFileLocation.MzDataAdmin)
                                        {
                                            writer.WriteStartElement("sourceFile");

                                            writer.WriteElementString("nameOfFile", Path.GetFileName(pseudoMsgfFilePath));
                                            writer.WriteElementString("pathToFile", pseudoMsgfFilePath);
                                            writer.WriteElementString("fileType", "MSGF file");

                                            writer.WriteEndElement();  // sourceFile
                                            skipNode = true;
                                        }
                                        break;

                                    case "software":
                                        if (eFileLocation == eMSGFReportXMLFileLocation.MzDataDataProcessing)
                                        {
                                            CreateMSGFReportXmlFileWriteSoftwareVersion(xmlReader, writer, dataPkgJob.PeptideHitResultType);
                                            skipNode = true;
                                        }
                                        break;

                                    case "instrumentName":
                                        if (eFileLocation == eMSGFReportXMLFileLocation.MzDataInstrument)
                                        {
                                            // Write out the actual instrument name
                                            writer.WriteElementString("instrumentName", dataPkgJob.Instrument);
                                            skipNode = true;

                                            instrumentDetailsAutoDefined = WriteXMLInstrumentInfo(writer, dataPkgJob.InstrumentGroup);
                                        }
                                        break;

                                    case "source":
                                    case "analyzerList":
                                    case "detector":
                                        if (eFileLocation == eMSGFReportXMLFileLocation.MzDataInstrument && instrumentDetailsAutoDefined)
                                        {
                                            skipNode = true;
                                        }
                                        break;

                                    case "cvParam":
                                        if (eFileLocation == eMSGFReportXMLFileLocation.ExperimentAdditional)
                                        {
                                            // Override the cvParam if it has Accession PRIDE:0000175

                                            writer.WriteStartElement("cvParam");

                                            if (xmlReader.HasAttributes)
                                            {
                                                var valueOverride = string.Empty;
                                                xmlReader.MoveToFirstAttribute();
                                                do
                                                {
                                                    if (xmlReader.Name == "accession" && xmlReader.Value == "PRIDE:0000175")
                                                    {
                                                        valueOverride = "DMS PRIDE_Converter " +
                                                            System.Reflection.Assembly.GetExecutingAssembly().GetName().Version;
                                                    }

                                                    if (xmlReader.Name == "value" && valueOverride.Length > 0)
                                                    {
                                                        writer.WriteAttributeString(xmlReader.Name, valueOverride);
                                                    }
                                                    else
                                                    {
                                                        writer.WriteAttributeString(xmlReader.Name, xmlReader.Value);
                                                    }
                                                } while (xmlReader.MoveToNextAttribute());
                                            }

                                            writer.WriteEndElement();  // cvParam
                                            skipNode = true;
                                        }
                                        break;

                                    case "Identifications":
                                        if (!CreateMSGFReportXMLFileWriteIDs(writer, pseudoMSGFData, orgDBNameGenerated))
                                        {
                                            LogError("CreateMSGFReportXMLFileWriteIDs returned false; aborting");
                                            return string.Empty;
                                        }

                                        if (!CreateMSGFReportXMLFileWriteProteins(writer, orgDBNameGenerated))
                                        {
                                            LogError("CreateMSGFReportXMLFileWriteProteins returned false; aborting");
                                            return string.Empty;
                                        }

                                        skipNode = true;
                                        break;

                                    case "Fasta":
                                        // This section is written out by CreateMSGFReportXMLFileWriteIDs
                                        skipNode = true;
                                        break;

                                    case "PTMs":
                                        // In the future, we might write out customized PTMs in CreateMSGFReportXMLFileWriteProteins
                                        // For now, just copy over whatever is in the template msgf-report.xml file
                                        skipNode = false;
                                        break;

                                    case "DatabaseMappings":

                                        writer.WriteStartElement("DatabaseMappings");
                                        writer.WriteStartElement("DatabaseMapping");

                                        writer.WriteElementString("SearchEngineDatabaseName", orgDBNameGenerated);
                                        writer.WriteElementString("SearchEngineDatabaseVersion", "Unknown");

                                        writer.WriteElementString("CuratedDatabaseName", proteinCollectionListOrFasta);
                                        writer.WriteElementString("CuratedDatabaseVersion", "1");

                                        writer.WriteEndElement();      // DatabaseMapping
                                        writer.WriteEndElement();      // DatabaseMappings

                                        skipNode = true;
                                        break;

                                    case "ConfigurationOptions":
                                        writer.WriteStartElement("ConfigurationOptions");

                                        WriteConfigurationOption(writer, "search_engine", "MSGF");
                                        WriteConfigurationOption(writer, "peptide_threshold", udtFilterThresholds.PValueThreshold.ToString("0.00"));
                                        WriteConfigurationOption(writer, "add_carbamidomethylation", "false");

                                        writer.WriteEndElement();      // ConfigurationOptions

                                        skipNode = true;
                                        break;
                                }

                                if (skipNode)
                                {
                                    if (xmlReader.NodeType != XmlNodeType.EndElement)
                                    {
                                        // Skip this element (and any children nodes enclosed in this elemnt)
                                        // Likely should not do this when xmlReader.NodeType is XmlNodeType.EndElement
                                        xmlReader.Skip();
                                    }
                                }
                                else
                                {
                                    // Copy this element from the source file to the target file

                                    writer.WriteStartElement(xmlReader.Name);

                                    if (xmlReader.HasAttributes)
                                    {
                                        xmlReader.MoveToFirstAttribute();
                                        do
                                        {
                                            if (attributeOverride.Count > 0 && attributeOverride.TryGetValue(xmlReader.Name, out var overrideValue))
                                            {
                                                writer.WriteAttributeString(xmlReader.Name, overrideValue);
                                            }
                                            else
                                            {
                                                writer.WriteAttributeString(xmlReader.Name, xmlReader.Value);
                                            }
                                        } while (xmlReader.MoveToNextAttribute());

                                        elementCloseDepths.Push(xmlReader.Depth);
                                    }
                                    else if (xmlReader.IsEmptyElement)
                                    {
                                        writer.WriteEndElement();
                                    }
                                }
                                break;

                            case XmlNodeType.EndElement:

                                if (recentElements.Count > 10)
                                    recentElements.Dequeue();
                                recentElements.Enqueue("EndElement " + xmlReader.Name);

                                while (elementCloseDepths.Count > 0 && elementCloseDepths.Peek() > xmlReader.Depth + 1)
                                {
                                    elementCloseDepths.Pop();
                                    writer.WriteEndElement();
                                }

                                writer.WriteEndElement();

                                if (xmlReader.Name == "MzDataDescription")
                                {
                                    insideMzDataDescription = false;
                                }

                                while (elementCloseDepths.Count > 0 && elementCloseDepths.Peek() > xmlReader.Depth)
                                {
                                    elementCloseDepths.Pop();
                                }
                                break;

                            case XmlNodeType.Text:

                                if (!string.IsNullOrEmpty(xmlReader.Value))
                                {
                                    if (recentElements.Count > 10)
                                        recentElements.Dequeue();
                                    if (xmlReader.Value.Length > 10)
                                    {
                                        recentElements.Enqueue(xmlReader.Value.Substring(0, 10));
                                    }
                                    else
                                    {
                                        recentElements.Enqueue(xmlReader.Value);
                                    }
                                }

                                writer.WriteString(xmlReader.Value);
                                break;
                        }
                    }

                    writer.WriteEndDocument();
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in CreateMSGFReportXMLFile", ex);

                var recentElementNames = string.Empty;
                foreach (var item in recentElements)
                {
                    if (string.IsNullOrEmpty(recentElementNames))
                    {
                        recentElementNames = string.Copy(item);
                    }
                    else
                    {
                        recentElementNames += "; " + item;
                    }
                }

                LogDebug(recentElementNames);

                return string.Empty;
            }

            return prideReportXMLFilePath;
        }

        private bool CreateMSGFReportXMLFileWriteIDs(XmlWriter writer,
            IReadOnlyDictionary<string, List<udtPseudoMSGFDataType>> pseudoMSGFData, string orgDBNameGenerated)
        {
            try
            {
                writer.WriteStartElement("Identifications");

                foreach (var proteinEntry in pseudoMSGFData)
                {

                    if (!mCachedProteins.TryGetValue(proteinEntry.Key, out var indexAndSequence))
                    {
                        // Protein not found in mCachedProteins; this is unexpected (should have already been added by CreatePseudoMSGFFileUsingPHRPReader()
                        // Add the protein to mCachedProteins and mCachedProteinPSMCounts, though we won't know its sequence

                        indexAndSequence = new KeyValuePair<int, string>(mCachedProteins.Count, string.Empty);
                        mCachedProteinPSMCounts.Add(indexAndSequence.Key, proteinEntry.Value.Count);
                        mCachedProteins.Add(proteinEntry.Key, indexAndSequence);
                    }
                    else
                    {
                        mCachedProteinPSMCounts[indexAndSequence.Key] = proteinEntry.Value.Count;
                    }

                    writer.WriteStartElement("Identification");

                    // Protein name
                    writer.WriteElementString("Accession", proteinEntry.Key);

                    // Cleaned-up version of the Protein name; for example, for ref|NP_035862.2 we would put "NP_035862" here
                    // writer.WriteElementString("CuratedAccession", proteinEntry.Key);

                    // Protein name
                    writer.WriteElementString("UniqueIdentifier", proteinEntry.Key);

                    // Accession version would be determined when curating the "Accession" name.  For example, for ref|NP_035862.2 we would put "2" here
                    // writer.WriteElementString("AccessionVersion", "1");
                    writer.WriteElementString("Database", orgDBNameGenerated);

                    writer.WriteElementString("DatabaseVersion", "Unknown");

                    // Write out each PSM for this protein
                    foreach (var udtPeptide in proteinEntry.Value)
                    {
                        writer.WriteStartElement("Peptide");

                        writer.WriteElementString("Sequence", udtPeptide.CleanSequence);
                        writer.WriteElementString("CuratedSequence", string.Empty);
                        writer.WriteElementString("Start", "0");
                        writer.WriteElementString("End", "0");
                        writer.WriteElementString("SpectrumReference", udtPeptide.ScanNumber.ToString());

                        // Could write out details of dynamic mods
                        //    Would need to update DMS to include the PSI-Compatible mod names, descriptions, and masses.
                        //    However, since we're now submitting .mzid.gz files to PRIDE and not .msgf-report.xml files, this update is not necessary
                        //
                        // XML format:
                        // <ModificationItem>
                        //     <ModLocation>10</ModLocation>
                        //     <ModAccession>MOD:00425</ModAccession>
                        //     <ModDatabase>MOD</ModDatabase>
                        //     <ModMonoDelta>15.994915</ModMonoDelta>
                        //     <additional>
                        //         <cvParam cvLabel="MOD" accession="MOD:00425" name="monohydroxylated residue" value="15.994915" />
                        //     </additional>
                        // </ModificationItem>

                        writer.WriteElementString("isSpecific", "false");

                        // I wanted to record ResultID here, but we instead have to record Scan Number; otherwise PRIDE Converter Crashes
                        writer.WriteElementString("UniqueIdentifier", udtPeptide.ScanNumber.ToString());

                        writer.WriteStartElement("additional");

                        WriteCVParam(writer, "PRIDE", "PRIDE:0000065", "Upstream flanking sequence", udtPeptide.PrefixResidue);
                        WriteCVParam(writer, "PRIDE", "PRIDE:0000066", "Downstream flanking sequence", udtPeptide.SuffixResidue);

                        WriteCVParam(writer, "MS", "MS:1000041", "charge state", udtPeptide.ChargeState.ToString());
                        WriteCVParam(writer, "MS", "MS:1000042", "peak intensity", "0.0");
                        WriteCVParam(writer, "MS", "MS:1001870", "p-value for peptides", udtPeptide.PValue);

                        WriteUserParam(writer, "MQScore", udtPeptide.MQScore);
                        WriteUserParam(writer, "TotalPRMScore", udtPeptide.TotalPRMScore);

                        // WriteUserParam(writer, "MedianPRMScore", "0.0")
                        // WriteUserParam(writer, "FractionY", "0.0")
                        // WriteUserParam(writer, "FractionB", "0.0")

                        WriteUserParam(writer, "NTT", udtPeptide.NTT.ToString());

                        // WriteUserParam(writer, "F-Score", "0.0")

                        WriteUserParam(writer, "DeltaScore", udtPeptide.DeltaScore);
                        WriteUserParam(writer, "DeltaScoreOther", udtPeptide.DeltaScoreOther);
                        WriteUserParam(writer, "SpecProb", udtPeptide.MSGFSpecEValue);

                        writer.WriteEndElement();      // additional

                        writer.WriteEndElement();      // Peptide
                    }

                    // Protein level-scores
                    writer.WriteElementString("Score", "0.0");
                    writer.WriteElementString("Threshold", "0.0");
                    writer.WriteElementString("SearchEngine", "MSGF");

                    writer.WriteStartElement("additional");
                    writer.WriteEndElement();

                    writer.WriteElementString("FastaSequenceReference", indexAndSequence.Key.ToString());

                    writer.WriteEndElement();      // Identification
                }

                writer.WriteEndElement();          // Identifications
            }
            catch (Exception ex)
            {
                LogError("Exception in CreateMSGFReportXMLFileWriteIDs", ex);
                return false;
            }

            return true;
        }

        private bool CreateMSGFReportXMLFileWriteProteins(XmlWriter writer, string orgDBNameGenerated)
        {
            try
            {
                writer.WriteStartElement("Fasta");
                writer.WriteAttributeString("sourceDb", orgDBNameGenerated);
                writer.WriteAttributeString("sourceDbVersion", "Unknown");

                // Step through mCachedProteins
                // For each entry, the key is the protein name
                // The value is itself a key-value pair, where Value.Key is the protein index and Value.Value is the protein sequence

                foreach (var entry in mCachedProteins)
                {
                    var proteinName = string.Copy(entry.Key);
                    var proteinIndex = entry.Value.Key;

                    // Only write out this protein if it had 1 or more PSMs
                    if (!mCachedProteinPSMCounts.TryGetValue(proteinIndex, out var psmCount) || psmCount <= 0)
                        continue;

                    writer.WriteStartElement("Sequence");
                    writer.WriteAttributeString("id", proteinIndex.ToString());
                    writer.WriteAttributeString("accession", proteinName);

                    writer.WriteValue(entry.Value.Value);

                    writer.WriteEndElement();          // Sequence
                }

                writer.WriteEndElement();          // Fasta

                // In the future, we might write out customized PTMs here
                // For now, just copy over whatever is in the template msgf-report.xml file
                //
                // writer.WriteStartElement("PTMs")
                // writer.WriteFullEndElement()
            }
            catch (Exception ex)
            {
                LogError("Exception in CreateMSGFReportXMLFileWriteProteins", ex);
                return false;
            }

            return true;
        }

        private void CreateMSGFReportXmlFileWriteSoftwareVersion(XmlReader xmlReader, XmlWriter writer,
            clsPHRPReader.ePeptideHitResultType PeptideHitResultType)
        {
            var toolName = string.Empty;
            var toolVersion = string.Empty;
            var toolComments = string.Empty;
            var nodeDepth = xmlReader.Depth;

            // Read the name, version, and comments elements under software
            while (xmlReader.Read())
            {
                var error = false;
                switch (xmlReader.NodeType)
                {
                    case XmlNodeType.Element:
                        switch (xmlReader.Name)
                        {
                            case "name":
                                toolName = xmlReader.ReadElementContentAsString();
                                break;
                            case "version":
                                toolVersion = xmlReader.ReadElementContentAsString();
                                break;
                            case "comments":
                                toolComments = xmlReader.ReadElementContentAsString();
                                break;
                        }
                        break;

                    case XmlNodeType.EndElement:
                        if (xmlReader.Depth <= nodeDepth)
                        {
                            error = true;
                        }
                        break;
                }

                if (error)
                {
                    break;
                }
            }

            if (string.IsNullOrEmpty(toolName))
            {
                toolName = PeptideHitResultType.ToString();
                toolVersion = string.Empty;
                toolComments = string.Empty;
            }
            else
            {
                if (PeptideHitResultType == clsPHRPReader.ePeptideHitResultType.MSGFDB && toolName.ToUpper().StartsWith("MSGF"))
                {
                    // Tool Version in the template file is likely correct; use it
                }
                else if (PeptideHitResultType == clsPHRPReader.ePeptideHitResultType.Sequest && toolName.ToUpper().StartsWith("SEQUEST"))
                {
                    // Tool Version in the template file is likely correct; use it
                }
                else if (PeptideHitResultType == clsPHRPReader.ePeptideHitResultType.XTandem && toolName.ToUpper().Contains("TANDEM"))
                {
                    // Tool Version in the template file is likely correct; use it
                }
                else
                {
                    // Tool Version is likely not known
                    toolName = PeptideHitResultType.ToString();
                    toolVersion = string.Empty;
                    toolComments = string.Empty;
                }
            }

            writer.WriteStartElement("software");

            writer.WriteElementString("name", toolName);
            writer.WriteElementString("version", toolVersion);
            writer.WriteElementString("comments", toolComments);

            writer.WriteEndElement();  // software
        }

        /// <summary>
        /// Create the .msgf-pride.xml file using the .msgf-report.xml file
        /// </summary>
        /// <param name="job"></param>
        /// <param name="dataset"></param>
        /// <param name="prideReportXMLFilePath"></param>
        /// <param name="prideXmlFilePath">Output parameter: the full path of the newly created .msgf-pride.xml file</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        private bool CreatePrideXMLFile(int job, string dataset, string prideReportXMLFilePath, out string prideXmlFilePath)
        {

            prideXmlFilePath = string.Empty;

            try
            {
                var xmlFileName = Path.GetFileName(prideReportXMLFilePath);
                if (xmlFileName == null)
                {
                    LogError("Exception in CreatePrideXMLFile for job " + job + "; unable to determine the file name in " + prideReportXMLFilePath);
                    return false;
                }

                var baseFileName = xmlFileName.Replace(FILE_EXTENSION_MSGF_REPORT_XML, string.Empty);
                var msgfResultsFilePath = Path.Combine(m_WorkDir, baseFileName + FILE_EXTENSION_PSEUDO_MSGF);
                var mzXMLFilePath = Path.Combine(m_WorkDir, dataset + clsAnalysisResources.DOT_MZXML_EXTENSION);
                prideReportXMLFilePath = Path.Combine(m_WorkDir, baseFileName + FILE_EXTENSION_MSGF_REPORT_XML);

                var currentTask = "Running PRIDE Converter for job " + job + ", " + dataset;
                if (m_DebugLevel >= 1)
                {
                    LogMessage(currentTask);
                }

                var success = RunPrideConverter(job, dataset, msgfResultsFilePath, mzXMLFilePath, prideReportXMLFilePath);

                if (!success)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        LogError("Unknown error calling RunPrideConverter", m_message);
                    }
                }
                else
                {
                    // Make sure the result file was created
                    prideXmlFilePath = Path.Combine(m_WorkDir, baseFileName + FILE_EXTENSION_MSGF_PRIDE_XML);
                    if (!File.Exists(prideXmlFilePath))
                    {
                        LogError("Pride XML file not created for job " + job + ": " + prideXmlFilePath);
                        return false;
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception in CreatePrideXMLFile for job " + job, ex);
                return false;
            }

        }

        private bool CreatePXSubmissionFile(IReadOnlyDictionary<string, string> templateParameters)
        {
            const string TBD = "******* UPDATE ****** ";

            var filterText = string.Empty;

            try
            {
                var pXFilePath = Path.Combine(m_WorkDir, "PX_Submission_" + DateTime.Now.ToString("yyyy-MM-dd_HH-mm") + ".px");

                var prideXmlFilesCreated = CountResultFilesByType(clsPXFileInfoBase.ePXFileType.Result);
                var rawFilesStored = CountResultFilesByType(clsPXFileInfoBase.ePXFileType.Raw);
                var peakFilesStored = CountResultFilesByType(clsPXFileInfoBase.ePXFileType.Peak);
                var mzIDFilesStored = CountResultFilesByType(clsPXFileInfoBase.ePXFileType.ResultMzId);

                if (m_DebugLevel >= 1)
                {
                    LogMessage("Creating PXSubmission file: " + pXFilePath);
                    LogDebug(" Result stats: " + prideXmlFilesCreated + " Result (.msgf-pride.xml) files");
                    LogDebug(" Result stats: " + rawFilesStored + " Raw files");
                    LogDebug(" Result stats: " + peakFilesStored + " Peak (.mgf) files");
                    LogDebug(" Result stats: " + mzIDFilesStored + " Search (.mzid.gz) files");
                }

                string submissionType;
                if (mzIDFilesStored == 0 && prideXmlFilesCreated == 0)
                {
                    submissionType = PARTIAL_SUBMISSION;
                    LogMessage("Did not create any Pride XML result files; submission type is " + submissionType);
                }
                else if (prideXmlFilesCreated > 0 && mzIDFilesStored > prideXmlFilesCreated)
                {
                    submissionType = PARTIAL_SUBMISSION;
                    LogMessage("Stored more Search (.mzid.gz) files than Pride XML result files; submission type is " + submissionType);
                }
                else if (prideXmlFilesCreated > 0 && rawFilesStored > prideXmlFilesCreated)
                {
                    submissionType = PARTIAL_SUBMISSION;
                    LogMessage("Stored more Raw files than Pride XML result files; submission type is " + submissionType);
                }
                else if (mzIDFilesStored == 0)
                {
                    submissionType = PARTIAL_SUBMISSION;
                    LogMessage("Did not have any .mzid.gz files and did not create any Pride XML result files; submission type is " + submissionType);
                }
                else
                {
                    submissionType = COMPLETE_SUBMISSION;

                    if (mFilterThresholdsUsed.UseFDRThreshold || mFilterThresholdsUsed.UsePepFDRThreshold || mFilterThresholdsUsed.UseMSGFSpecEValue)
                    {
                        const string filterTextBase = "msgf-pride.xml files are filtered on ";
                        filterText = string.Empty;

                        if (mFilterThresholdsUsed.UseFDRThreshold)
                        {
                            if (string.IsNullOrEmpty(filterText))
                            {
                                filterText = filterTextBase;
                            }
                            else
                            {
                                filterText += " and ";
                            }

                            filterText += (mFilterThresholdsUsed.FDRThreshold * 100).ToString("0.0") + "% FDR at the PSM level";
                        }

                        if (mFilterThresholdsUsed.UsePepFDRThreshold)
                        {
                            if (string.IsNullOrEmpty(filterText))
                            {
                                filterText = filterTextBase;
                            }
                            else
                            {
                                filterText += " and ";
                            }

                            filterText += (mFilterThresholdsUsed.PepFDRThreshold * 100).ToString("0.0") + "% FDR at the peptide level";
                        }

                        if (mFilterThresholdsUsed.UseMSGFSpecEValue)
                        {
                            if (string.IsNullOrEmpty(filterText))
                            {
                                filterText = filterTextBase;
                            }
                            else
                            {
                                filterText += " and ";
                            }

                            filterText += "MSGF Spectral Probability <= " + mFilterThresholdsUsed.MSGFSpecEValueThreshold.ToString("0.0E+00");
                        }
                    }
                }

                var paramsWithCVs = new SortedSet<string>
                {
                    "experiment_type",
                    "species",
                    "tissue",
                    "instrument",
                    "cell_type",
                    "disease",
                    "quantification",
                    "modification"
                };

                using (var swPXFile = new StreamWriter(new FileStream(pXFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    WritePXHeader(swPXFile, "submitter_name", "Matthew Monroe", templateParameters, paramsWithCVs);
                    WritePXHeader(swPXFile, "submitter_email", "matthew.monroe@pnnl.gov", templateParameters, paramsWithCVs);
                    WritePXHeader(swPXFile, "submitter_affiliation", PNNL_NAME_COUNTRY, templateParameters, paramsWithCVs);
                    WritePXHeader(swPXFile, "submitter_pride_login", "matthew.monroe@pnnl.gov", templateParameters, paramsWithCVs);

                    WritePXHeader(swPXFile, "lab_head_name", "Richard D. Smith", templateParameters, paramsWithCVs);
                    WritePXHeader(swPXFile, "lab_head_email", "dick.smith@pnnl.gov", templateParameters, paramsWithCVs);
                    WritePXHeader(swPXFile, "lab_head_affiliation", PNNL_NAME_COUNTRY, templateParameters, paramsWithCVs);

                    WritePXHeader(swPXFile, "project_title", TBD + "User-friendly Article Title", templateParameters, paramsWithCVs);

                    // Minimum 50 characterse, max 5000 characters
                    WritePXHeader(swPXFile, "project_description", TBD + "Summary sentence", templateParameters, paramsWithCVs, 50);

                    // We don't normally use the project_tag field, so it is commented out
                    // Example official tags are:
                    //  Human proteome project
                    //  Human plasma project
                    // WritePXHeader(swPXFile, "project_tag", TBD + "Official project tag assigned by the repository", templateParameters)

                    if (templateParameters.ContainsKey("pubmed_id"))
                    {
                        WritePXHeader(swPXFile, "pubmed_id", TBD, templateParameters, paramsWithCVs);
                    }

                    // We don't normally use this field, so it is commented out
                    // WritePXHeader(swPXFile, "other_omics_link", "Related data is available from PeptideAtlas at http://www.peptideatlas.org/PASS/PASS00297")

                    // Comma separated list; suggest at least 3 keywords
                    WritePXHeader(swPXFile, "keywords", TBD, templateParameters, paramsWithCVs);

                    // Minimum 50 characters, max 5000 characters
                    WritePXHeader(swPXFile, "sample_processing_protocol", TBD, templateParameters, paramsWithCVs, 50);

                    // Minimum 50 characters, max 5000 characters
                    WritePXHeader(swPXFile, "data_processing_protocol", TBD, templateParameters, paramsWithCVs, 50);

                    // Example values for experiment_type (a given submission can have more than one experiment_type listed)
                    //   [PRIDE, PRIDE:0000427, Top-down proteomics, ]
                    //   [PRIDE, PRIDE:0000429, Shotgun proteomics, ]
                    //   [PRIDE, PRIDE:0000430, Chemical cross-linking coupled with mass spectrometry proteomics, ]
                    //   [PRIDE, PRIDE:0000433, Affinity purification coupled with mass spectrometry proteomics, ]
                    //   [PRIDE, PRIDE:0000311, SRM/MRM, ]
                    //   [PRIDE, PRIDE:0000447, SWATH MS, ]
                    //   [PRIDE, PRIDE:0000451, MSE, ]
                    //   [PRIDE, PRIDE:0000452, HDMSE, ]
                    //   [PRIDE, PRIDE:0000453, PAcIFIC, ]
                    //   [PRIDE, PRIDE:0000454, All-ion fragmentation, ]
                    //   [MS, MS:1002521, Mass spectrometry imaging,]
                    //   [MS, MS:1002521, Mass spectrometry imaging,]

                    var defaultExperiment = GetCVString("PRIDE", "PRIDE:0000429", "Shotgun proteomics");
                    WritePXHeader(swPXFile, "experiment_type", defaultExperiment, templateParameters, paramsWithCVs);

                    WritePXLine(swPXFile, new List<string>
                    {
                        "MTD",
                        "submission_type",
                        submissionType
                    });

                    if (submissionType == COMPLETE_SUBMISSION)
                    {
                        // Note that the comment field has been deprecated in v2.x of the px file
                        // However, we don't have a good alternative place to put this comment, so we'll include it anyway
                        if (!string.IsNullOrWhiteSpace(filterText))
                        {
                            WritePXHeader(swPXFile, "comment", filterText, paramsWithCVs);
                        }
                    }
                    else
                    {
                        var comment = "Data produced by the DMS Processing pipeline using ";
                        if (mSearchToolsUsed.Count == 1)
                        {
                            comment += "search tool " + mSearchToolsUsed.First();
                        }
                        else if (mSearchToolsUsed.Count == 2)
                        {
                            comment += "search tools " + mSearchToolsUsed.First() + " and " + mSearchToolsUsed.Last();
                        }
                        else if (mSearchToolsUsed.Count > 2)
                        {
                            comment += "search tools " + string.Join(", ", (
                                from item in mSearchToolsUsed where item != mSearchToolsUsed.Last() orderby item select item).ToList());
                            comment += ", and " + mSearchToolsUsed.Last();
                        }

                        WritePXHeader(swPXFile, "reason_for_partial", comment, paramsWithCVs);
                    }

                    var mouseOrHuman = false;
                    if (mExperimentNEWTInfo.Count == 0)
                    {
                        // None of the data package jobs had valid NEWT info
                        var defaultSpecies = TBD + GetCVString("NEWT", "2323", "unclassified Bacteria");
                        WritePXHeader(swPXFile, "species", defaultSpecies, templateParameters, paramsWithCVs);
                    }
                    else
                    {
                        // NEWT info is defined; write it out
                        foreach (var item in mExperimentNEWTInfo)
                        {
                            WritePXHeader(swPXFile, "species", GetNEWTCv(item.Key, item.Value), paramsWithCVs);

                            if (item.Value.IndexOf("Homo sapiens", StringComparison.OrdinalIgnoreCase) >= 0 ||
                                item.Value.IndexOf("Mus musculus", StringComparison.OrdinalIgnoreCase) >= 0)
                            {
                                mouseOrHuman = true;
                            }
                        }
                    }

                    if (mExperimentTissue.Count > 0)
                    {
                        foreach (var item in mExperimentTissue)
                        {
                            WritePXHeader(swPXFile, "tissue", GetCVString("BTO", item.Key, item.Value), templateParameters, paramsWithCVs);
                        }
                    }
                    else
                    {
                        string defaultTissue;
                        if (mouseOrHuman)
                            defaultTissue = TBD + DEFAULT_TISSUE_CV_MOUSE_HUMAN;
                        else
                            defaultTissue = TBD + DEFAULT_TISSUE_CV;

                        WritePXHeader(swPXFile, "tissue", defaultTissue, templateParameters, paramsWithCVs);
                    }

                    var defaultCellType = TBD + "Optional, e.g. " + DEFAULT_CELL_TYPE_CV + DELETION_WARNING;
                    var defaultDisease = TBD + "Optional, e.g. " + DEFAULT_DISEASE_TYPE_CV + DELETION_WARNING;

                    WritePXHeader(swPXFile, "cell_type", defaultCellType, templateParameters, paramsWithCVs);
                    WritePXHeader(swPXFile, "disease", defaultDisease, templateParameters, paramsWithCVs);

                    // Example values for quantification (a given submission can have more than one type listed)
                    //   [PRIDE, PRIDE:0000318, 18O,]
                    //   [PRIDE, PRIDE:0000320, AQUA,]
                    //   [PRIDE, PRIDE:0000319, ICAT,]
                    //   [PRIDE, PRIDE:0000321, ICPL,]
                    //   [PRIDE, PRIDE:0000315, SILAC,]
                    //   [PRIDE, PRIDE:0000314, TMT,]
                    //   [PRIDE, PRIDE:0000313, iTRAQ,]
                    //   [PRIDE, PRIDE:0000323, TIC,]
                    //   [PRIDE, PRIDE:0000322, emPAI,]
                    //   [PRIDE, PRIDE:0000435, Peptide counting,]
                    //   [PRIDE, PRIDE:0000436, Spectral counting,]
                    //   [PRIDE, PRIDE:0000437, Protein Abundance Index – PAI,]
                    //   [PRIDE, PRIDE:0000438, Spectrum count/molecular weight,]
                    //   [PRIDE, PRIDE:0000439, Spectral Abundance Factor – SAF,]
                    //   [PRIDE, PRIDE:0000440, Normalized Spectral Abundance Factor – NSAF,]
                    //   [PRIDE, PRIDE:0000441, APEX - Absolute Protein Expression,]
                    var defaultQuantCV = TBD + "Optional, e.g. " + DEFAULT_QUANTIFICATION_TYPE_CV;
                    WritePXHeader(swPXFile, "quantification", defaultQuantCV, templateParameters, paramsWithCVs);

                    if (mInstrumentGroupsStored.Count > 0)
                    {
                        WritePXInstruments(swPXFile, paramsWithCVs);
                    }
                    else
                    {
                        // Instrument type is unknown
                        var defaultInstrument = TBD + GetCVString("MS", "MS:1000031", "instrument model", "CUSTOM UNKNOWN MASS SPEC");
                        WritePXHeader(swPXFile, "instrument", defaultInstrument, templateParameters, paramsWithCVs);
                    }

                    // Note that the modification terms are optional for complete submissions
                    // However, it doesn't hurt to include them
                    WritePXMods(swPXFile, paramsWithCVs);

                    // Could write additional terms here
                    // WritePXHeader(swPXFile, "additional", GetCVString("", "", "Patient", "Colorectal cancer patient 1"), templateParameters)

                    // If this is a re-submission or re-analysis, use these:
                    // WritePXHeader(swPXFile, "resubmission_px", "PXD00001", templateParameters)
                    // WritePXHeader(swPXFile, "reanalysis_px", "PXD00001", templateParameters)

                    // Add a blank line
                    swPXFile.WriteLine();

                    // Write the header row for the files
                    WritePXLine(swPXFile, new List<string>
                    {
                        "FMH",
                        "file_id",
                        "file_type",
                        "file_path",
                        "file_mapping"
                    });

                    var fileInfoCols = new List<string>();

                    // Keys in this dictionary are fileIDs, values are file names
                    var resultFileIDs = new Dictionary<int, string>();

                    // Append the files and mapping information to the ProteomeXchange PX file
                    foreach (var item in mPxResultFiles)
                    {
                        fileInfoCols.Clear();

                        fileInfoCols.Add("FME");

                        // file_id
                        fileInfoCols.Add(item.Key.ToString());
                        var fileTypeName = PXFileTypeName(item.Value.PXFileType);

                        // file_type; allowed values are result, raw, peak, search, quantification, gel, other
                        fileInfoCols.Add(fileTypeName);

                        // file_path
                        fileInfoCols.Add(Path.Combine(@"D:\Upload", m_ResFolderName, item.Value.Filename));

                        var fileMappings = new List<string>();
                        foreach (var mapID in item.Value.FileMappings)
                        {
                            // file_mapping
                            fileMappings.Add(mapID.ToString());
                        }

                        fileInfoCols.Add(string.Join(",", fileMappings));

                        WritePXLine(swPXFile, fileInfoCols);

                        if (fileTypeName == "RESULT")
                        {
                            resultFileIDs.Add(item.Key, item.Value.Filename);
                        }
                    }

                    // Determine whether the tissue or cell_type columns will bein the SMH section
                    var smhIncludesCellType = DictionaryHasDefinedValue(templateParameters, "cell_type");
                    var smhIncludesDisease = DictionaryHasDefinedValue(templateParameters, "disease");

                    var reJobAddon = new Regex(@"(_Job\d+)(_msgfplus)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                    swPXFile.WriteLine();

                    // Write the header row for the SMH section
                    var columnNames = new List<string>
                    {
                        "SMH",
                        "file_id",
                        "species",
                        "tissue",
                        "cell_type",
                        "disease",
                        "modification",
                        "instrument",
                        "quantification",
                        "experimental_factor"
                    };

                    WritePXLine(swPXFile, columnNames);

                    // Add the SME lines below the SMH line
                    foreach (var resultFile in resultFileIDs)
                    {
                        fileInfoCols.Clear();

                        fileInfoCols.Add("SME");

                        // file_id
                        fileInfoCols.Add(resultFile.Key.ToString());

                        var resultFileName = resultFile.Value;

                        if (!mMzIdSampleInfo.TryGetValue(resultFile.Value, out var sampleMetadata))
                        {
                            // Result file name may have been customized to include _Job1000000
                            // Check for this, and update resultFileName if required

                            var reMatch = reJobAddon.Match(resultFileName);
                            if (reMatch.Success)
                            {
                                var resultFileNameNew = resultFileName.Substring(0, reMatch.Index) + reMatch.Groups[2].Value +
                                                        resultFileName.Substring(reMatch.Index + reMatch.Length);
                                resultFileName = resultFileNameNew;
                            }
                        }

                        if (mMzIdSampleInfo.TryGetValue(resultFileName, out sampleMetadata))
                        {
                            fileInfoCols.Add(sampleMetadata.Species);
                            fileInfoCols.Add(sampleMetadata.Tissue);

                            if (smhIncludesCellType)
                            {
                                fileInfoCols.Add(sampleMetadata.CellType);
                            }
                            else
                            {
                                fileInfoCols.Add(string.Empty);
                            }

                            if (smhIncludesDisease)
                            {
                                fileInfoCols.Add(sampleMetadata.Disease);
                            }
                            else
                            {
                                fileInfoCols.Add(string.Empty);
                            }

                            var mods = string.Empty;
                            foreach (var modEntry in sampleMetadata.Modifications)
                            {
                                if (mods.Length > 0)
                                    mods += ", ";
                                mods += GetCVString(modEntry.Value);
                            }

                            // modification
                            fileInfoCols.Add(mods);

                            GetInstrumentAccession(sampleMetadata.InstrumentGroup, out var instrumentAccession, out var instrumentDescription);

                            var instrumentCV = GetInstrumentCv(instrumentAccession, instrumentDescription);

                            fileInfoCols.Add(instrumentCV);

                            fileInfoCols.Add(GetValueOrDefault("quantification)", templateParameters, sampleMetadata.Quantification));

                            fileInfoCols.Add(sampleMetadata.ExperimentalFactor);
                        }
                        else
                        {
                            LogWarning(" Sample Metadata not found for " + resultFile.Value);
                        }

                        WritePXLine(swPXFile, fileInfoCols);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in CreatePXSubmissionFile", ex);
                return false;
            }

            return true;
        }

        private void DefineFilesToSkipTransfer()
        {
            m_jobParams.AddResultFileExtensionToSkip(FILE_EXTENSION_PSEUDO_MSGF);
            m_jobParams.AddResultFileExtensionToSkip(FILE_EXTENSION_MSGF_REPORT_XML);
            m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX);

            m_jobParams.AddResultFileToSkip("PRIDEConverter_ConsoleOutput.txt");
            m_jobParams.AddResultFileToSkip("PRIDEConverter_Version.txt");

            var diWorkDir = new DirectoryInfo(m_WorkDir);
            foreach (var fiFile in diWorkDir.GetFiles(clsDataPackageFileHandler.JOB_INFO_FILE_PREFIX + "*.txt"))
            {
                m_jobParams.AddResultFileToSkip(fiFile.Name);
            }
        }

        private bool DefineProgramPaths()
        {
            // javaProgLoc will typically be "C:\Program Files\Java\jre7\bin\Java.exe"
            var javaProgLoc = GetJavaProgLoc();
            if (string.IsNullOrEmpty(javaProgLoc))
            {
                return false;
            }

            // Determine the path to the PRIDEConverter program
            mPrideConverterProgLoc = DetermineProgramLocation("PRIDEConverterProgLoc", "pride-converter-2.0-SNAPSHOT.jar");

            if (string.IsNullOrEmpty(mPrideConverterProgLoc))
            {
                if (string.IsNullOrEmpty(m_message))
                {
                    LogError("Error determining PrideConverter program location");
                }
                return false;
            }

            mMSXmlGeneratorAppPath = GetMSXmlGeneratorAppPath();

            return true;
        }

        private bool DefinePxFileMapping(int fileID, int parentFileID)
        {

            if (!mPxResultFiles.TryGetValue(fileID, out var oPXFileInfo))
            {
                LogError("FileID " + fileID + " not found in mPxResultFiles; unable to add parent file");
                return false;
            }

            oPXFileInfo.AddFileMapping(parentFileID);

            return true;
        }

        private bool DictionaryHasDefinedValue(IReadOnlyDictionary<string, string> templateParameters, string termName)
        {
            if (templateParameters.TryGetValue(termName, out var value))
            {
                if (!string.IsNullOrWhiteSpace(value))
                    return true;
            }

            return false;
        }

        private bool FileExistsInTransferFolder(string remoteTransferFolder, string filePath, string optionalSuffix = "")
        {
            var fileName = Path.GetFileName(filePath);
            if (fileName == null)
                return false;

            if (File.Exists(Path.Combine(remoteTransferFolder, fileName)))
                return true;

            if (string.IsNullOrWhiteSpace(optionalSuffix))
            {
                return false;
            }

            return File.Exists(Path.Combine(remoteTransferFolder, fileName + optionalSuffix));
        }

        private string GetCVString(clsSampleMetadata.udtCvParamInfoType cvParamInfo)
        {
            return GetCVString(cvParamInfo.CvRef, cvParamInfo.Accession, cvParamInfo.Name, cvParamInfo.Value);
        }

        private string GetCVString(string cvRef, string accession, string name, string value = "")
        {
            if (string.IsNullOrEmpty(value))
            {
                value = string.Empty;
            }
            else if (value.Length > 200)
            {
                LogWarning("CV value parameter truncated since too long: " + value);
                value = value.Substring(0, 200);
            }

            return "[" + cvRef + ", " + accession + ", " + name + ", " + value + "]";
        }

        private string GetInstrumentCv(string accession, string description)
        {
            string instrumentCV;

            if (string.IsNullOrEmpty(accession))
            {
                instrumentCV = GetCVString("MS", "MS:1000031", "instrument model", "CUSTOM UNKNOWN MASS SPEC");
            }
            else
            {
                instrumentCV = GetCVString("MS", accession, description);
            }

            return instrumentCV;
        }

        private string GetNEWTCv(int newtID, string newtName)
        {
            if (newtID == 0 && string.IsNullOrWhiteSpace(newtName))
            {
                newtID = 2323;
                newtName = "unclassified Bacteria";
            }

            return GetCVString("NEWT", newtID.ToString(), newtName);
        }

        /// <summary>
        /// Determines the Accession and Desription for the given instrument group
        /// </summary>
        /// <param name="instrumentGroup"></param>
        /// <param name="accession">Output parameter</param>
        /// <param name="description">Output parameter</param>
        /// <remarks></remarks>
        private void GetInstrumentAccession(string instrumentGroup, out string accession, out string description)
        {
            accession = string.Empty;
            description = string.Empty;

            switch (instrumentGroup)
            {
                case "Agilent_GC-MS":
                    // This is an Agilent 7890A with a 5975C detector
                    // The closest match is an LC/MS system
                    accession = "MS:1000471";
                    description = "6140 Quadrupole LC/MS";
                    break;
                case "Agilent_TOF_V2":
                    accession = "MS:1000472";
                    description = "6210 Time-of-Flight LC/MS";
                    break;
                case "Bruker_Amazon_Ion_Trap":
                    accession = "MS:1001542";
                    description = "amaZon ETD";

                    break;
                case "Bruker_FTMS":
                    accession = "MS:1001549";
                    description = "solariX";
                    break;
                case "Bruker_QTOF":
                    accession = "MS:1001537";
                    description = "BioTOF";
                    break;
                case "Exactive":
                    accession = "MS:1000649";
                    description = "Exactive";
                    break;
                case "TSQ":
                case "GC-TSQ":
                    // TSQ_3 and TSQ_4 are TSQ Vantage instruments
                    accession = "MS:1001510";
                    description = "TSQ Vantage";
                    break;
                case "LCQ":
                    accession = "MS:1000554";
                    description = "LCQ Deca";
                    break;
                case "LTQ":
                case "LTQ-Prep":
                    accession = "MS:1000447";
                    description = "LTQ";
                    break;
                case "LTQ-ETD":
                    accession = "MS:1000638";
                    description = "LTQ XL ETD";
                    break;
                case "LTQ-FT":
                    accession = "MS:1000448";
                    description = "LTQ FT";
                    break;
                case "Lumos":
                    accession = "MS:1002732";
                    description = "Orbitrap Fusion Lumos";
                    break;
                case "Orbitrap":
                    accession = "MS:1000449";
                    description = "LTQ Orbitrap";
                    break;
                case "QExactive":
                    accession = "MS:1001911";
                    description = "Q Exactive";
                    break;
                case "Sciex_QTrap":
                    accession = "MS:1000931";
                    description = "QTRAP 5500";
                    break;
                case "Sciex_TripleTOF":
                    accession = "MS:1000932";
                    description = "TripleTOF 5600";
                    break;
                case "VelosOrbi":
                    accession = "MS:1001742";
                    description = "LTQ Orbitrap Velos";
                    break;
                case "VelosPro":
                    // Note that VPro01 is actually a Velos Pro
                    accession = "MS:1000855";
                    description = "LTQ Velos";
                    break;
            }
        }

        private string GetPrideConverterVersion(string prideConverterProgLoc)
        {
            var prideConverterVersion = "unknown";

            mCmdRunner = new clsRunDosProgram(m_WorkDir, m_DebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            m_StatusTools.CurrentOperation = "Determining PrideConverter Version";
            m_StatusTools.UpdateAndWrite(m_progress);
            var versionFilePath = Path.Combine(m_WorkDir, "PRIDEConverter_Version.txt");

            var cmdStr = "-jar " + PossiblyQuotePath(prideConverterProgLoc);

            cmdStr += " -converter -version";

            if (m_DebugLevel >= 2)
            {
                LogDebug(mJavaProgLoc + " " + cmdStr);
            }

            mCmdRunner.CreateNoWindow = false;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = false;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = versionFilePath;
            mCmdRunner.WorkDir = m_WorkDir;

            var success = mCmdRunner.RunProgram(mJavaProgLoc, cmdStr, "PrideConverter", true);

            // Assure that the console output file has been parsed
            ParseConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath);

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (!success)
            {
                LogError("Error running PrideConverter to determine its version");
            }
            else
            {
                var fiVersionFile = new FileInfo(versionFilePath);

                if (fiVersionFile.Exists)
                {
                    // Open the version file and read the version
                    using (var srVersionFile = new StreamReader(new FileStream(fiVersionFile.FullName, FileMode.Open, FileAccess.Read)))
                    {
                        if (!srVersionFile.EndOfStream)
                        {
                            prideConverterVersion = srVersionFile.ReadLine();
                        }
                    }
                }
            }

            return prideConverterVersion;
        }

        private string GetValueOrDefault(string type, IReadOnlyDictionary<string, string> parameters, string defaultValue)
        {
            if (parameters.TryGetValue(type, out var valueOverride))
            {
                return valueOverride;
            }

            return defaultValue;
        }

        private udtFilterThresholdsType InitializeOptions()
        {
            // Update the processing options
            mCreatePrideXMLFiles = m_jobParams.GetJobParameter("CreatePrideXMLFiles", false);

            mCreateMSGFReportFilesOnly = m_jobParams.GetJobParameter("CreateMSGFReportFilesOnly", false);
            mCreateMGFFiles = m_jobParams.GetJobParameter("CreateMGFFiles", true);

            mIncludePepXMLFiles = m_jobParams.GetJobParameter("IncludePepXMLFiles", true);
            mProcessMzIdFiles = m_jobParams.GetJobParameter("IncludeMzIdFiles", true);

            if (mCreateMSGFReportFilesOnly)
            {
                mCreateMGFFiles = false;
                mIncludePepXMLFiles = false;
                mProcessMzIdFiles = false;
                mCreatePrideXMLFiles = false;
            }

            mCachedOrgDBName = string.Empty;

            // Initialize the protein dictionaries
            mCachedProteins = new Dictionary<string, KeyValuePair<int, string>>();
            mCachedProteinPSMCounts = new Dictionary<int, int>();

            // Initialize the PXFile lists
            mPxMasterFileList = new Dictionary<string, clsPXFileInfoBase>(StringComparer.OrdinalIgnoreCase);
            mPxResultFiles = new Dictionary<int, clsPXFileInfo>();

            // Initialize the CDTAFileStats dictionary
            mCDTAFileStats = new Dictionary<string, clsPXFileInfoBase>(StringComparer.OrdinalIgnoreCase);

            // Clear the previous dataset objects
            mPreviousDatasetName = string.Empty;
            mPreviousDatasetFilesToDelete = new List<string>();
            mPreviousDatasetFilesToCopy = new List<string>();

            // Initialize additional items
            mFilterThresholdsUsed = new udtFilterThresholdsType();
            mInstrumentGroupsStored = new Dictionary<string, SortedSet<string>>();
            mSearchToolsUsed = new SortedSet<string>();
            mExperimentNEWTInfo = new Dictionary<int, string>();
            mExperimentTissue = new Dictionary<string, string>();

            mModificationsUsed = new Dictionary<string, clsSampleMetadata.udtCvParamInfoType>(StringComparer.OrdinalIgnoreCase);

            mMzIdSampleInfo = new Dictionary<string, clsSampleMetadata>(StringComparer.OrdinalIgnoreCase);

            // Determine the filter thresholds
            var udtFilterThresholds = new udtFilterThresholdsType();
            udtFilterThresholds.Clear();
            udtFilterThresholds.PValueThreshold = m_jobParams.GetJobParameter("PValueThreshold", udtFilterThresholds.PValueThreshold);
            udtFilterThresholds.FDRThreshold = m_jobParams.GetJobParameter("FDRThreshold", udtFilterThresholds.FDRThreshold);
            udtFilterThresholds.PepFDRThreshold = m_jobParams.GetJobParameter("PepFDRThreshold", udtFilterThresholds.PepFDRThreshold);

            // Support both SpecProb and SpecEValue job parameters
            udtFilterThresholds.MSGFSpecEValueThreshold = m_jobParams.GetJobParameter("MSGFSpecProbThreshold", udtFilterThresholds.MSGFSpecEValueThreshold);
            udtFilterThresholds.MSGFSpecEValueThreshold = m_jobParams.GetJobParameter("MSGFSpecEvalueThreshold", udtFilterThresholds.MSGFSpecEValueThreshold);

            udtFilterThresholds.UseFDRThreshold = m_jobParams.GetJobParameter("UseFDRThreshold", udtFilterThresholds.UseFDRThreshold);
            udtFilterThresholds.UsePepFDRThreshold = m_jobParams.GetJobParameter("UsePepFDRThreshold", udtFilterThresholds.UsePepFDRThreshold);

            // Support both SpecProb and SpecEValue job parameters
            udtFilterThresholds.UseMSGFSpecEValue = m_jobParams.GetJobParameter("UseMSGFSpecProb", udtFilterThresholds.UseMSGFSpecEValue);
            udtFilterThresholds.UseMSGFSpecEValue = m_jobParams.GetJobParameter("UseMSGFSpecEValue", udtFilterThresholds.UseMSGFSpecEValue);

            return udtFilterThresholds;
        }

        /// <summary>
        /// Returns True if the there are multiple jobs in mDataPackagePeptideHitJobs for the dataset for the specified job
        /// </summary>
        /// <param name="job"></param>
        /// <returns>True if this job's dataset has multiple jobs in mDataPackagePeptideHitJobs, otherwise False</returns>
        /// <remarks></remarks>
        private bool JobFileRenameRequired(int job)
        {
            if (!mDataPackagePeptideHitJobs.TryGetValue(job, out var dataPkgJob))
                return false;

            var dataset = dataPkgJob.Dataset;

            var jobsForDataset = (from item in mDataPackagePeptideHitJobs where item.Value.Dataset == dataset select item).ToList().Count;

            return jobsForDataset > 1;
        }

        private bool LookupDataPackagePeptideHitJobs()
        {
            if (mDataPackagePeptideHitJobs == null)
            {
                mDataPackagePeptideHitJobs = new Dictionary<int, clsDataPackageJobInfo>();
            }
            else
            {
                mDataPackagePeptideHitJobs.Clear();
            }

            var dataPackagePeptideHitJobs = RetrieveDataPackagePeptideHitJobInfo(out var additionalJobs, out var errorMsg);
            if (dataPackagePeptideHitJobs.Count == 0)
            {
                var msg = "Error loading data package job info";
                if (string.IsNullOrEmpty(errorMsg))
                    LogError(msg + ": RetrieveDataPackagePeptideHitJobInfo returned no jobs");
                else
                    LogError(msg + ": " + errorMsg);

                return false;
            }

            var jobsToUse = ExtractPackedJobParameterList(clsAnalysisResourcesPRIDEConverter.JOB_PARAM_DATA_PACKAGE_PEPTIDE_HIT_JOBS);

            if (jobsToUse.Count == 0)
            {
                LogWarning("Packed job parameter " + clsAnalysisResourcesPRIDEConverter.JOB_PARAM_DATA_PACKAGE_PEPTIDE_HIT_JOBS +
                           " is empty; no jobs to process");
            }
            else
            {
                var dataPackageJobs = new Dictionary<int, clsDataPackageJobInfo>();
                foreach (var item in dataPackagePeptideHitJobs)
                {
                    if (!dataPackageJobs.ContainsKey(item.Job))
                        dataPackageJobs.Add(item.Job, item);
                }

                foreach (var item in additionalJobs)
                {
                    if (!dataPackageJobs.ContainsKey(item.Job))
                        dataPackageJobs.Add(item.Job, item);
                }

                // Populate mDataPackagePeptideHitJobs using the jobs in jobsToUse
                foreach (var job in jobsToUse)
                {
                    if (!int.TryParse(job, out var jobNumber))
                        continue;

                    if (dataPackageJobs.TryGetValue(jobNumber, out var dataPkgJob))
                    {
                        mDataPackagePeptideHitJobs.Add(jobNumber, dataPkgJob);
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// Parse the PRIDEConverter console output file to determine the PRIDE Version
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            // Example Console output:
            //
            // 2012-11-20 16:58:47,333 INFO ReportUnmarshallerFactory - Unmarshaller Initialized
            // 2012-11-20 16:58:47,333 INFO ReportReader - Creating index:
            // 2012-11-20 16:58:49,860 INFO ReportMarshallerFactory - Marshaller Initialized
            // Writing PRIDE XML to F:\DMS_WorkDir5\AID_MAC_001_R1_20Nov07_Draco_07-07-19_Job863734.msgf-pride.xml
            // 2012-11-20 16:58:49,860 INFO PrideXmlWriter - DAO Configuration: {search_engine=MSGF, peptide_threshold=0.05, add_carbamidomethylation=false}
            // 2012-11-20 16:58:49,860 WARN PrideXmlWriter - Writing file : F:\DMS_WorkDir5\AID_MAC_001_R1_20Nov07_Draco_07-07-19_Job863734.msgf-pride.xml
            // 2012-11-20 16:59:01,124 INFO PrideXmlWriter - Marshalled 1000 spectra
            // 2012-11-20 16:59:01,124 INFO PrideXmlWriter - Used: 50 Free: 320 Heap size: 371 Xmx: 2728
            // 2012-11-20 16:59:02,231 INFO PrideXmlWriter - Marshalled 2000 spectra
            // 2012-11-20 16:59:02,231 INFO PrideXmlWriter - Used: 214 Free: 156 Heap size: 371 Xmx: 2728
            // 2012-11-20 16:59:03,152 INFO PrideXmlWriter - Marshalled 3000 spectra
            // 2012-11-20 16:59:03,152 INFO PrideXmlWriter - Used: 128 Free: 223 Heap size: 351 Xmx: 2728
            // 2012-11-20 16:59:04,103 INFO PrideXmlWriter - Marshalled 4000 spectra
            // 2012-11-20 16:59:04,103 INFO PrideXmlWriter - Used: 64 Free: 278 Heap size: 342 Xmx: 2728
            // 2012-11-20 16:59:05,258 INFO PrideXmlWriter - Marshalled 5000 spectra
            // 2012-11-20 16:59:05,258 INFO PrideXmlWriter - Used: 21 Free: 312 Heap size: 333 Xmx: 2728
            // 2012-11-20 16:59:06,693 ERROR StandardXpathAccess - The index does not contain any entry for the requested xpath: /Report/PTMs/PTM

            try
            {
                if (!File.Exists(consoleOutputFilePath))
                {
                    if (m_DebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + consoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
                {
                    LogDebug("Parsing file " + consoleOutputFilePath);
                }

                mConsoleOutputErrorMsg = string.Empty;

                using (var srInFile = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    mConsoleOutputErrorMsg = string.Empty;

                    while (!srInFile.EndOfStream)
                    {
                        var lineIn = srInFile.ReadLine();

                        if (!string.IsNullOrWhiteSpace(lineIn))
                        {
                            if (lineIn.ToLower().Contains(" error "))
                            {
                                if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                                {
                                    mConsoleOutputErrorMsg = "Error running Pride Converter:";
                                }
                                mConsoleOutputErrorMsg += "; " + lineIn;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        /// <summary>
        /// Process one job
        /// </summary>
        /// <param name="jobInfo">Keys are job numbers and values contain job info</param>
        /// <param name="udtFilterThresholds"></param>
        /// <param name="analysisResults"></param>
        /// <param name="dataPackageDatasets"></param>
        /// <param name="remoteTransferFolder"></param>
        /// <param name="datasetRawFilePaths"></param>
        /// <param name="templateParameters"></param>
        /// <param name="assumeInstrumentDataUnpurged"></param>
        /// <returns></returns>
        private CloseOutType ProcessJob(
            KeyValuePair<int, clsDataPackageJobInfo> jobInfo,
            udtFilterThresholdsType udtFilterThresholds,
            clsAnalysisResults analysisResults,
            IReadOnlyDictionary<int, clsDataPackageDatasetInfo> dataPackageDatasets,
            string remoteTransferFolder,
            IReadOnlyDictionary<string, string> datasetRawFilePaths,
            IReadOnlyDictionary<string, string> templateParameters,
            bool assumeInstrumentDataUnpurged)
        {
            bool success;
            var resultFiles = new clsResultFileContainer();

            var job = jobInfo.Value.Job;
            var dataset = jobInfo.Value.Dataset;

            if (mPreviousDatasetName != dataset)
            {
                TransferPreviousDatasetFiles(analysisResults, remoteTransferFolder);

                // Retrieve the dataset files for this dataset
                mPreviousDatasetName = dataset;

                if (mCreatePrideXMLFiles && !mCreateMSGFReportFilesOnly)
                {
                    // Create the .mzXML files if it is missing
                    success = CreateMzXMLFileIfMissing(dataset, analysisResults, datasetRawFilePaths);
                    if (!success)
                    {
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }
                }
            }

            // Update the cached analysis tool names
            if (!mSearchToolsUsed.Contains(jobInfo.Value.Tool))
            {
                mSearchToolsUsed.Add(jobInfo.Value.Tool);
            }

            // Look for this job's dataset in dataPackageDatasets
            var datasetId = jobInfo.Value.DatasetID;

            if (dataPackageDatasets.TryGetValue(datasetId, out var datasetInfo))
            {
                if (!string.IsNullOrWhiteSpace(datasetInfo.Experiment_Tissue_ID))
                {
                    AddExperimentTissueId(datasetInfo.Experiment_Tissue_ID, datasetInfo.Experiment_Tissue_Name);
                }
                else
                {
                    datasetInfo = null;
                }
            }

            // Update the cached NEWT info
            AddNEWTInfo(jobInfo.Value.Experiment_NEWT_ID, jobInfo.Value.Experiment_NEWT_Name);

            // Retrieve the PHRP files, MSGF+ results, and _dta.txt or .mzML.gz file for this job
            var filesCopied = new List<string>();

            success = RetrievePHRPFiles(job, dataset, analysisResults, remoteTransferFolder, filesCopied);
            if (!success)
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            var searchedMzML = false;
            foreach (var copiedFile in filesCopied)
            {
                if (copiedFile.EndsWith(DOT_MZML, StringComparison.OrdinalIgnoreCase) ||
                    copiedFile.EndsWith(DOT_MZML_GZ, StringComparison.OrdinalIgnoreCase))
                {
                    searchedMzML = true;
                    break;
                }
            }

            resultFiles.MGFFilePath = string.Empty;
            if (mCreateMGFFiles && !searchedMzML)
            {
                if (FileExistsInTransferFolder(remoteTransferFolder, dataset + DOT_MGF))
                {
                    // The .mgf file already exists on the remote server; upate .MGFFilePath
                    // The path to the file doesn't matter; just the name
                    resultFiles.MGFFilePath = Path.Combine(m_WorkDir, dataset + DOT_MGF);
                }
                else
                {
                    // Convert the _dta.txt file to .mgf files
                    success = ConvertCDTAToMGF(jobInfo.Value, out var mgfPath);
                    resultFiles.MGFFilePath = mgfPath;
                    if (!success)
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

            }
            else
            {
                // Store the path to the _dta.txt or .mzML.gz file
                if (searchedMzML)
                {
                    resultFiles.MGFFilePath = Path.Combine(m_WorkDir, dataset + DOT_MZML_GZ);
                }
                else
                {
                    resultFiles.MGFFilePath = Path.Combine(m_WorkDir, dataset + "_dta.txt");
                }

                if (!assumeInstrumentDataUnpurged && !searchedMzML && !File.Exists(resultFiles.MGFFilePath))
                {
                    // .mgf file not found
                    // We don't check for .mzML.gz files since those are not copied locally if they already exist in remoteTransferFolder
                    resultFiles.MGFFilePath = string.Empty;
                }
            }

            // Update the .mzid.gz file(s) for this job

            if (mProcessMzIdFiles && jobInfo.Value.PeptideHitResultType == clsPHRPReader.ePeptideHitResultType.MSGFDB)
            {
                m_message = string.Empty;

                success = UpdateMzIdFiles(remoteTransferFolder, jobInfo.Value, datasetInfo, searchedMzML, out var mzIdFilePaths, out _, templateParameters);

                if (!success || mzIdFilePaths == null || mzIdFilePaths.Count == 0)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        LogError("UpdateMzIdFiles returned false for job " + job + ", dataset " + dataset);
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                resultFiles.MzIDFilePaths.Clear();

                foreach (var mzidFilePath in mzIdFilePaths)
                {
                    var mzidFile = new FileInfo(mzidFilePath);
                    resultFiles.MzIDFilePaths.Add(mzidFile.FullName);
                }
            }

            if (mIncludePepXMLFiles && jobInfo.Value.PeptideHitResultType != clsPHRPReader.ePeptideHitResultType.Unknown ||
                jobInfo.Value.PeptideHitResultType == clsPHRPReader.ePeptideHitResultType.Sequest)
            {
                var pepXmlFilename = jobInfo.Value.Dataset + ".pepXML";
                var pepXMLFile = new FileInfo(Path.Combine(m_WorkDir, pepXmlFilename));
                if (pepXMLFile.Exists)
                {
                    // Make sure it is capitalized correctly, then gzip it

                    if (!string.Equals(pepXMLFile.Name, pepXmlFilename, StringComparison.Ordinal))
                    {
                        pepXMLFile.MoveTo(pepXMLFile.FullName + ".tmp");
                        pepXMLFile.MoveTo(Path.Combine(m_WorkDir, pepXmlFilename));
                    }

                    // Note that the original file will be auto-deleted after the .gz file is created
                    var gzippedPepXMLFile = GZipFile(pepXMLFile);

                    if (gzippedPepXMLFile == null)
                    {
                        if (string.IsNullOrEmpty(m_message))
                        {
                            LogError("GZipFile returned false for " + pepXMLFile.FullName);
                        }
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    resultFiles.PepXMLFile = gzippedPepXMLFile.FullName;
                }
            }

            // Store the instrument group and instrument name
            StoreInstrumentInfo(jobInfo.Value);

            resultFiles.PrideXmlFilePath = string.Empty;

            if (mCreatePrideXMLFiles)
            {
                // Create the .msgf-report.xml file for this job

                success = CreateMSGFReportFile(job, dataset, udtFilterThresholds, out var prideReportXMLFilePath);
                if (!success)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                AddToListIfNew(mPreviousDatasetFilesToDelete, prideReportXMLFilePath);

                if (!mCreateMSGFReportFilesOnly)
                {
                    // Create the .msgf-Pride.xml file for this job
                    success = CreatePrideXMLFile(job, dataset, prideReportXMLFilePath, out var prideXmlPath);
                    resultFiles.PrideXmlFilePath = prideXmlPath;
                    if (!success)
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }

            success = AppendToPXFileInfo(jobInfo.Value, datasetRawFilePaths, resultFiles);

            if (success)
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            return CloseOutType.CLOSEOUT_FAILED;

        }

        private string PXFileTypeName(clsPXFileInfoBase.ePXFileType ePXFileType)
        {
            switch (ePXFileType)
            {
                case clsPXFileInfoBase.ePXFileType.Result:
                case clsPXFileInfoBase.ePXFileType.ResultMzId:
                    return "RESULT";
                case clsPXFileInfoBase.ePXFileType.Raw:
                    return "RAW";
                case clsPXFileInfoBase.ePXFileType.Search:
                    return "SEARCH";
                case clsPXFileInfoBase.ePXFileType.Peak:
                    return "PEAK";
                default:
                    return "OTHER";
            }
        }

        /// <summary>
        /// Reads the template PX Submission file
        /// Caches the keys and values for the method lines (which start with MTD)
        /// </summary>
        /// <returns>Dictionary of keys and values</returns>
        /// <remarks></remarks>
        private Dictionary<string, string> ReadTemplatePXSubmissionFile()
        {
            const string OBSOLETE_FIELD_FLAG = "SKIP_OBSOLETE_FIELD";

            var parameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            var keyNameOverrides = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                {"name", "submitter_name"},
                {"email", "submitter_email"},
                {"affiliation", "submitter_affiliation"},
                {"title", "project_title"},
                {"description", "project_description"},
                {"type", "submission_type"},
                {"comment", OBSOLETE_FIELD_FLAG},
                {"pride_login", "submitter_pride_login"},
                {"pubmed", "pubmed_id"}
            };

            try
            {
                var templateFileName = clsAnalysisResourcesPRIDEConverter.GetPXSubmissionTemplateFilename(m_jobParams, WarnIfJobParamMissing: false);
                var templateFilePath = Path.Combine(m_WorkDir, templateFileName);

                if (!File.Exists(templateFilePath))
                {
                    return parameters;
                }

                using (var srTemplateFile = new StreamReader(new FileStream(templateFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srTemplateFile.EndOfStream)
                    {
                        var lineIn = srTemplateFile.ReadLine();

                        if (string.IsNullOrEmpty(lineIn))
                            continue;

                        if (!lineIn.StartsWith("MTD"))
                            continue;

                        var columns = lineIn.Split(new[] { '\t' }, 3).ToList();

                        if (columns.Count < 3 || string.IsNullOrEmpty(columns[1]))
                            continue;

                        var keyName = columns[1];

                        // Automatically rename parameters updated from v1.x to v2.x of the .px file format
                        if (keyNameOverrides.TryGetValue(keyName, out var keyNameNew))
                        {
                            keyName = keyNameNew;
                        }

                        if (!string.Equals(keyName, OBSOLETE_FIELD_FLAG) && !parameters.ContainsKey(keyName))
                        {
                            parameters.Add(keyName, columns[2].Trim());
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Error in ReadTemplatePXSubmissionFile", ex);
                return parameters;
            }

            return parameters;
        }

        private clsSampleMetadata.udtCvParamInfoType ReadWriteCvParam(XmlReader xmlReader, XmlWriter writer,
            Stack<int> elementCloseDepths)
        {
            var udtCvParam = new clsSampleMetadata.udtCvParamInfoType();
            udtCvParam.Clear();

            writer.WriteStartElement(xmlReader.Name);

            if (xmlReader.HasAttributes)
            {
                xmlReader.MoveToFirstAttribute();
                do
                {
                    writer.WriteAttributeString(xmlReader.Name, xmlReader.Value);

                    switch (xmlReader.Name)
                    {
                        case "accession":
                            udtCvParam.Accession = xmlReader.Value;
                            break;
                        case "cvRef":
                            udtCvParam.CvRef = xmlReader.Value;
                            break;
                        case "name":
                            udtCvParam.Name = xmlReader.Value;
                            break;
                        case "value":
                            udtCvParam.Value = xmlReader.Value;
                            break;
                        case "unitCvRef":
                            udtCvParam.unitCvRef = xmlReader.Value;
                            break;
                        case "unitName":
                            udtCvParam.unitName = xmlReader.Value;
                            break;
                        case "unitAccession":
                            udtCvParam.unitAccession = xmlReader.Value;
                            break;
                    }
                } while (xmlReader.MoveToNextAttribute());

                elementCloseDepths.Push(xmlReader.Depth);
            }
            else if (xmlReader.IsEmptyElement)
            {
                writer.WriteEndElement();
            }

            return udtCvParam;
        }

        private bool RetrievePHRPFiles(
            int job,
            string dataset,
            clsAnalysisResults analysisResults,
            string remoteTransferFolder,
            ICollection<string> filesCopied)
        {
            var filesToCopy = new List<string>();

            try
            {
                var jobInfoFilePath = clsDataPackageFileHandler.GetJobInfoFilePath(job, m_WorkDir);

                if (!File.Exists(jobInfoFilePath))
                {
                    // Assume all of the files already exist
                    return true;
                }

                // Read the contents of the JobInfo file
                // It will be empty if no PHRP files are required
                using (var infoReader = new StreamReader(new FileStream(jobInfoFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!infoReader.EndOfStream)
                    {
                        filesToCopy.Add(infoReader.ReadLine());
                    }
                }

                // If filesToCopy only has a _dta.txt file and one or more .mzid.gz files, check the transfer folder for a .mgf file and a .mzid.gz file
                // If the .mgf and .mzid.gz file already exist; skip processing this job
                if (filesToCopy.Count >= 2)
                {
                    var cdtaFile = false;
                    var mzidFiles = new List<string>();
                    var otherFiles = new List<string>();

                    foreach (var sourceFilePath in filesToCopy)
                    {
                        if (sourceFilePath.EndsWith("_dta.zip", StringComparison.OrdinalIgnoreCase))
                            cdtaFile = true;
                        else if (sourceFilePath.EndsWith(DOT_MZID_GZ, StringComparison.OrdinalIgnoreCase))
                            mzidFiles.Add(Path.GetFileName(sourceFilePath));
                        else
                            otherFiles.Add(Path.GetFileName(sourceFilePath));
                    }

                    if (otherFiles.Count == 0 && cdtaFile && mzidFiles.Count > 0)
                    {
                        if (FileExistsInTransferFolder(remoteTransferFolder, dataset + DOT_MGF))
                        {
                            var allowSkip = mzidFiles.All(remoteMzIdFile => FileExistsInTransferFolder(remoteTransferFolder, remoteMzIdFile));

                            if (allowSkip)
                            {
                                LogDebug(string.Format("Skipping job {0} since the .mgf and .mzid.gz files already exist at {1}", job,
                                                       remoteTransferFolder));

                                foreach (var sourceFilePath in filesToCopy)
                                {
                                    filesCopied.Add(Path.GetFileName(sourceFilePath));
                                }

                                filesCopied.Add(dataset + DOT_MGF);
                                return true;
                            }
                        }
                    }
                }

                var fileCountNotFound = 0;

                // Retrieve the files
                // If the same dataset has multiple jobs, we might overwrite existing files;
                // that's OK since results files that we care about will have been auto-renamed based on the call to JobFileRenameRequired

                foreach (var sourceFilePath in filesToCopy)
                {
                    if (sourceFilePath.StartsWith(clsAnalysisResources.MYEMSL_PATH_FLAG))
                    {
                        // Make sure the myEMSLUtilities object knows about this dataset
                        m_MyEMSLUtilities.AddDataset(dataset);
                        DatasetInfoBase.ExtractMyEMSLFileID(sourceFilePath, out var cleanFilePath);

                        var fiSourceFileClean = new FileInfo(cleanFilePath);
                        var unzipRequired = string.Equals(fiSourceFileClean.Extension, ".zip", StringComparison.OrdinalIgnoreCase);

                        m_MyEMSLUtilities.AddFileToDownloadQueue(sourceFilePath, unzipRequired);

                        filesCopied.Add(fiSourceFileClean.Name);

                        continue;
                    }

                    var fiSourceFile = new FileInfo(sourceFilePath);

                    if (!fiSourceFile.Exists)
                    {
                        fileCountNotFound += 1;
                        LogError(string.Format("File not found for job {0}: {1}", job, sourceFilePath));
                        continue;
                    }

                    var targetFilePath = Path.Combine(m_WorkDir, fiSourceFile.Name);

                    var fiLocalFile = new FileInfo(targetFilePath);
                    var alreadyCopiedToTransferDirectory = false;

                    if (fiSourceFile.Name.EndsWith(DOT_MZML, StringComparison.OrdinalIgnoreCase) ||
                        fiSourceFile.Name.EndsWith(DOT_MZML_GZ, StringComparison.OrdinalIgnoreCase))
                    {
                        // mzML files can be large
                        // If the file already exists in the transfer directory and the sizes match, do not recopy

                        var fiFileInTransferDirectory = new FileInfo(Path.Combine(remoteTransferFolder, fiSourceFile.Name));

                        if (fiFileInTransferDirectory.Exists)
                        {
                            if (fiFileInTransferDirectory.Length == fiSourceFile.Length)
                            {
                                alreadyCopiedToTransferDirectory = true;
                                LogDebug(string.Format("Skipping file {0} since already copied to {1}", fiSourceFile.Name, remoteTransferFolder));
                            }
                        }
                    }

                    if (alreadyCopiedToTransferDirectory)
                    {
                        filesCopied.Add(fiSourceFile.Name);
                    }
                    else
                    {
                        // Retrieve the file, allowing for up to 3 attempts (uses CopyFileUsingLocks)
                        analysisResults.CopyFileWithRetry(fiSourceFile.FullName, fiLocalFile.FullName, true);

                        if (!fiLocalFile.Exists)
                        {
                            LogError("PHRP file was not copied locally: " + fiLocalFile.Name);
                            return false;
                        }

                        filesCopied.Add(fiSourceFile.Name);

                        var unzipped = false;

                        // Decrompress .zip files
                        // Do not decompress .gz files since we can decompress them on-the-fly while reading them
                        if (string.Equals(fiLocalFile.Extension, ".zip", StringComparison.OrdinalIgnoreCase))
                        {
                            // Decompress the .zip file
                            m_DotNetZipTools.UnzipFile(fiLocalFile.FullName, m_WorkDir);
                            unzipped = true;
                        }

                        if (unzipped)
                        {
                            foreach (var unzippedFile in m_DotNetZipTools.MostRecentUnzippedFiles)
                            {
                                filesCopied.Add(unzippedFile.Key);
                                AddToListIfNew(mPreviousDatasetFilesToDelete, unzippedFile.Value);
                            }
                        }
                    }

                    AddToListIfNew(mPreviousDatasetFilesToDelete, fiLocalFile.FullName);
                }

                if (m_MyEMSLUtilities.FilesToDownload.Count > 0)
                {
                    if (!m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkDir, Downloader.DownloadFolderLayout.FlatNoSubfolders))
                    {
                        if (string.IsNullOrWhiteSpace(m_message))
                        {
                            m_message = "ProcessMyEMSLDownloadQueue return false";
                        }
                        return false;
                    }

                    if (m_MyEMSLUtilities.FilesToDownload.Count > 0)
                    {
                        // The queue should have already been cleared; checking just in case
                        m_MyEMSLUtilities.ClearDownloadQueue();
                    }
                }

                if (fileCountNotFound == 0)
                    return true;

                return false;
            }
            catch (Exception ex)
            {
                LogError("Error in RetrievePHRPFiles", ex);
                return false;
            }
        }

        private bool RetrieveStoragePathInfoTargetFile(string storagePathInfoFilePath, clsAnalysisResults analysisResults,
            out string destPath)
        {
            return RetrieveStoragePathInfoTargetFile(storagePathInfoFilePath, analysisResults, IsFolder: false, destPath: out destPath);
        }

        private bool RetrieveStoragePathInfoTargetFile(string storagePathInfoFilePath, clsAnalysisResults analysisResults, bool IsFolder,
            out string destPath)
        {
            var sourceFilePath = string.Empty;

            destPath = string.Empty;

            try
            {
                if (!File.Exists(storagePathInfoFilePath))
                {
                    var msg = "StoragePathInfo file not found";
                    LogError(msg + ": " + storagePathInfoFilePath);
                    m_message = msg;
                    return false;
                }

                using (var srInfoFile = new StreamReader(new FileStream(storagePathInfoFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    if (!srInfoFile.EndOfStream)
                    {
                        sourceFilePath = srInfoFile.ReadLine();
                    }
                }

                if (string.IsNullOrEmpty(sourceFilePath))
                {
                    var msg = "StoragePathInfo file was empty";
                    LogError(msg + ": " + storagePathInfoFilePath);
                    m_message = msg;
                    return false;
                }

                destPath = Path.Combine(m_WorkDir, Path.GetFileName(sourceFilePath));

                if (IsFolder)
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
                LogError("Error in RetrieveStoragePathInfoTargetFile", ex);
                return false;
            }

            return true;
        }

        private bool RunPrideConverter(int job, string dataset, string msgfResultsFilePath, string mzXMLFilePath, string prideReportFilePath)
        {
            if (string.IsNullOrEmpty(msgfResultsFilePath))
            {
                LogError("msgfResultsFilePath has not been defined; unable to continue");
                return false;
            }

            if (string.IsNullOrEmpty(mzXMLFilePath))
            {
                LogError("mzXMLFilePath has not been defined; unable to continue");
                return false;
            }

            if (string.IsNullOrEmpty(prideReportFilePath))
            {
                LogError("prideReportFilePath has not been defined; unable to continue");
                return false;
            }

            mCmdRunner = new clsRunDosProgram(m_WorkDir, m_DebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            if (m_DebugLevel >= 1)
            {
                LogMessage("Running PrideConverter on " + Path.GetFileName(msgfResultsFilePath));
            }

            m_StatusTools.CurrentOperation = "Running PrideConverter";
            m_StatusTools.UpdateAndWrite(m_progress);

            var cmdStr = "-jar " + PossiblyQuotePath(mPrideConverterProgLoc);

            // QC_Shew_12_02_Run-03_18Jul12_Roc_12-04-08.msgf
            cmdStr += " -converter -mode convert -engine msgf -sourcefile " + PossiblyQuotePath(msgfResultsFilePath);

            // QC_Shew_12_02_Run-03_18Jul12_Roc_12-04-08.mzXML
            cmdStr += " -spectrafile " + PossiblyQuotePath(mzXMLFilePath);

            // QC_Shew_12_02_Run-03_18Jul12_Roc_12-04-08.msgf-report.xml
            cmdStr += " -reportfile " + PossiblyQuotePath(prideReportFilePath);

            cmdStr += " -reportOnlyIdentifiedSpectra";
            cmdStr += " -debug";

            LogDebug(mJavaProgLoc + " " + cmdStr);

            mCmdRunner.CreateNoWindow = false;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = false;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, PRIDEConverter_CONSOLE_OUTPUT);
            mCmdRunner.WorkDir = m_WorkDir;

            var success = mCmdRunner.RunProgram(mJavaProgLoc, cmdStr, "PrideConverter", true);

            // Assure that the console output file has been parsed
            ParseConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath);

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                if (mConsoleOutputErrorMsg.Contains("/Report/PTMs/PTM"))
                {
                    // Ignore this error
                    mConsoleOutputErrorMsg = string.Empty;
                }
                else
                {
                    LogError(mConsoleOutputErrorMsg);
                }
            }

            if (!success)
            {
                LogError("Error running PrideConverter, dataset " + dataset + ", job " + job);
            }

            return success;
        }

        private void StoreInstrumentInfo(clsDataPackageDatasetInfo datasetInfo)
        {
            StoreInstrumentInfo(datasetInfo.InstrumentGroup, datasetInfo.Instrument);
        }

        private void StoreInstrumentInfo(clsDataPackageJobInfo dataPkgJob)
        {
            StoreInstrumentInfo(dataPkgJob.InstrumentGroup, dataPkgJob.Instrument);
        }

        private void StoreInstrumentInfo(string instrumentGroup, string instrumentName)
        {
            if (mInstrumentGroupsStored.TryGetValue(instrumentGroup, out var instruments))
            {
                if (!instruments.Contains(instrumentName))
                {
                    instruments.Add(instrumentName);
                }
            }
            else
            {
                instruments = new SortedSet<string> { instrumentName };
                mInstrumentGroupsStored.Add(instrumentGroup, instruments);
            }
        }

        private void StoreMzIdSampleInfo(string mzIdFilePath, clsSampleMetadata sampleMetadata)
        {
            var fiFile = new FileInfo(mzIdFilePath);

            if (!mMzIdSampleInfo.ContainsKey(fiFile.Name))
            {
                mMzIdSampleInfo.Add(fiFile.Name, sampleMetadata);
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <param name="prideConverterProgLoc"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo(string prideConverterProgLoc)
        {
            var toolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo>();

            if (mCreatePrideXMLFiles)
            {
                var fiPrideConverter = new FileInfo(prideConverterProgLoc);
                if (!fiPrideConverter.Exists)
                {
                    try
                    {
                        toolVersionInfo = "Unknown";
                        return SetStepTaskToolVersion(toolVersionInfo, new List<FileInfo>());
                    }
                    catch (Exception ex)
                    {
                        var msg = "Exception calling SetStepTaskToolVersion: " + ex.Message;
                        LogError(msg);
                        return false;
                    }
                }

                // Run the PRIDE Converter using the -version switch to determine its version
                toolVersionInfo = GetPrideConverterVersion(fiPrideConverter.FullName);

                toolFiles.Add(fiPrideConverter);
            }
            else
            {
                // Lookup the version of the AnalysisManagerPrideConverter plugin
                if (!StoreToolVersionInfoForLoadedAssembly(ref toolVersionInfo, "AnalysisManagerPRIDEConverterPlugIn", includeRevision: false))
                {
                    return false;
                }
            }

            toolFiles.Add(new FileInfo(mMSXmlGeneratorAppPath));

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, saveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        private void TransferPreviousDatasetFiles(clsAnalysisResults analysisResults, string remoteTransferFolder)
        {
            // Delete the dataset files for the previous dataset
            var filesToRetry = new List<string>();

            if (mPreviousDatasetFilesToCopy.Count > 0)
            {
                filesToRetry.Clear();

                try
                {
                    // Copy the files we want to keep to the remote Transfer Directory
                    foreach (var srcFilePath in mPreviousDatasetFilesToCopy)
                    {
                        if (string.IsNullOrWhiteSpace(srcFilePath))
                            continue;

                        var targetFilePath = Path.Combine(remoteTransferFolder, Path.GetFileName(srcFilePath));

                        if (!File.Exists(srcFilePath))
                            continue;

                        if (string.Equals(srcFilePath, targetFilePath, StringComparison.OrdinalIgnoreCase))
                            continue;

                        try
                        {
                            analysisResults.CopyFileWithRetry(srcFilePath, targetFilePath, true);
                            AddToListIfNew(mPreviousDatasetFilesToDelete, srcFilePath);
                        }
                        catch (Exception ex)
                        {
                            LogError("Exception copying file to transfer directory", ex);
                            filesToRetry.Add(srcFilePath);
                        }
                    }
                }
                catch (Exception ex)
                {
                    // Folder creation error
                    LogError("Exception copying files to " + remoteTransferFolder, ex);
                    filesToRetry.AddRange(mPreviousDatasetFilesToCopy);
                }

                mPreviousDatasetFilesToCopy.Clear();

                if (filesToRetry.Count > 0)
                {
                    mPreviousDatasetFilesToCopy.AddRange(filesToRetry);

                    foreach (var item in filesToRetry)
                    {
                        if (mPreviousDatasetFilesToDelete.Contains(item, StringComparer.OrdinalIgnoreCase))
                        {
                            mPreviousDatasetFilesToDelete.Remove(item);
                        }
                    }
                }
            }

            if (mPreviousDatasetFilesToDelete.Count > 0)
            {
                filesToRetry.Clear();

                foreach (var item in mPreviousDatasetFilesToDelete)
                {
                    try
                    {
                        if (File.Exists(item))
                        {
                            File.Delete(item);
                        }
                    }
                    catch (Exception)
                    {
                        filesToRetry.Add(item);
                    }
                }

                mPreviousDatasetFilesToDelete.Clear();

                if (filesToRetry.Count > 0)
                {
                    mPreviousDatasetFilesToDelete.AddRange(filesToRetry);
                }
            }
        }

        private eMSGFReportXMLFileLocation UpdateMSGFReportXMLFileLocation(eMSGFReportXMLFileLocation eFileLocation, string elementName, bool insideMzDataDescription)
        {
            switch (elementName)
            {
                case "SearchResultIdentifier":
                    eFileLocation = eMSGFReportXMLFileLocation.SearchResultIdentifier;
                    break;
                case "Metadata":
                    eFileLocation = eMSGFReportXMLFileLocation.Metadata;
                    break;
                case "Protocol":
                    eFileLocation = eMSGFReportXMLFileLocation.Protocol;
                    break;
                case "admin":
                    if (insideMzDataDescription)
                    {
                        eFileLocation = eMSGFReportXMLFileLocation.MzDataAdmin;
                    }
                    break;
                case "instrument":
                    if (insideMzDataDescription)
                    {
                        eFileLocation = eMSGFReportXMLFileLocation.MzDataInstrument;
                    }
                    break;
                case "dataProcessing":
                    if (insideMzDataDescription)
                    {
                        eFileLocation = eMSGFReportXMLFileLocation.MzDataDataProcessing;
                    }
                    break;
                case "ExperimentAdditional":
                    eFileLocation = eMSGFReportXMLFileLocation.ExperimentAdditional;
                    break;
                case "Identifications":
                    eFileLocation = eMSGFReportXMLFileLocation.Identifications;
                    break;
                case "PTMs":
                    eFileLocation = eMSGFReportXMLFileLocation.PTMs;
                    break;
                case "DatabaseMappings":
                    eFileLocation = eMSGFReportXMLFileLocation.DatabaseMappings;
                    break;
                case "ConfigurationOptions":
                    eFileLocation = eMSGFReportXMLFileLocation.ConfigurationOptions;
                    break;
            }

            return eFileLocation;
        }

        /// <summary>
        /// Update the .mzid.gz file for the given job and dataset to have the correct Accession value for FileFormat
        /// Also update attributes location and name for element SpectraData if we converted _dta.txt files to .mgf files
        /// Lastly, remove any empty ModificationParams elements
        /// </summary>
        /// <param name="remoteTransferFolder">Remote transfer folder</param>
        /// <param name="dataPkgJob">Data package job info</param>
        /// <param name="dataPkgDatasetInfo">Dataset info for this job</param>
        /// <param name="searchedMzML">True if analysis job used a .mzML file (though we track .mzml.gz files with this class)</param>
        /// <param name="mzIdFilePaths">Output parameter: path to the .mzid.gz file for this job (will be multiple files if a SplitFasta search was performed)</param>
        /// <param name="mzIdExistsRemotely">Output parameter: true if the .mzid.gz file already exists in the remote transfer folder</param>
        /// <param name="templateParameters"></param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        private bool UpdateMzIdFiles(
            string remoteTransferFolder,
            clsDataPackageJobInfo dataPkgJob,
            clsDataPackageDatasetInfo dataPkgDatasetInfo,
            bool searchedMzML,
            out List<string> mzIdFilePaths,
            out bool mzIdExistsRemotely,
            IReadOnlyDictionary<string, string> templateParameters)
        {
            var sampleMetadata = new clsSampleMetadata();
            sampleMetadata.Clear();

            sampleMetadata.Species = ValidateCV(GetNEWTCv(dataPkgJob.Experiment_NEWT_ID, dataPkgJob.Experiment_NEWT_Name));

            var tissueCv = GetValueOrDefault("tissue", templateParameters, DEFAULT_TISSUE_CV);
            if (tissueCv.Contains("BRENDA"))
            {
                tissueCv = tissueCv.Replace("BRENDA", "BTO");
            }

            if (dataPkgDatasetInfo != null && !string.IsNullOrWhiteSpace(dataPkgDatasetInfo.Experiment_Tissue_ID))
            {
                tissueCv = GetCVString("BTO", dataPkgDatasetInfo.Experiment_Tissue_ID, dataPkgDatasetInfo.Experiment_Tissue_Name);
            }

            sampleMetadata.Tissue = ValidateCV(tissueCv);

            if (templateParameters.TryGetValue("cell_type", out var cellType))
            {
                sampleMetadata.CellType = ValidateCV(cellType);
            }
            else
            {
                sampleMetadata.CellType = string.Empty;
            }

            if (templateParameters.TryGetValue("disease", out var disease))
            {
                sampleMetadata.Disease = ValidateCV(disease);
            }
            else
            {
                sampleMetadata.Disease = string.Empty;
            }

            sampleMetadata.Modifications.Clear();
            sampleMetadata.InstrumentGroup = dataPkgJob.InstrumentGroup;
            sampleMetadata.Quantification = string.Empty;
            sampleMetadata.ExperimentalFactor = dataPkgJob.Experiment;

            mzIdFilePaths = new List<string>();

            try
            {
                // Open each .mzid.gz and parse it to create a new .mzid.gz file
                // Use a forward-only XML reader, copying most of the elements verbatim, but customizing some of them

                // For _dta.txt files, use <cvParam accession="MS:1001369" cvRef="PSI-MS" name="text file"/>
                // For .mgf files,     use <cvParam accession="MS:1001062" cvRef="PSI-MS" name="Mascot MGF file"/>
                // Will also need to update the location and name attributes of the SpectraData element
                // <SpectraData location="E:\DMS_WorkDir3\QC_Shew_08_04-pt5-2_11Jan09_Sphinx_08-11-18_dta.txt" name="QC_Shew_08_04-pt5-2_11Jan09_Sphinx_08-11-18_dta.txt" id="SID_1">

                // For split FASTA files each job step should have a custom .FASTA file, but we're ignoring that fact for now

                // If <ModificationParams /> is found, remove it

                var success = false;
                string mzIdFilePath;

                if (dataPkgJob.NumberOfClonedSteps > 0)
                {
                    mzIdExistsRemotely = false;

                    for (var splitFastaResultID = 1; splitFastaResultID <= dataPkgJob.NumberOfClonedSteps; splitFastaResultID++)
                    {
                        success = UpdateMzIdFile(
                            remoteTransferFolder,
                            dataPkgJob.Job,
                            dataPkgJob.Dataset,
                            searchedMzML,
                            splitFastaResultID,
                            sampleMetadata,
                            out mzIdFilePath,
                            out var splitMzIdExistsRemotely);

                        if (splitMzIdExistsRemotely)
                            mzIdExistsRemotely = true;

                        if (success)
                        {
                            mzIdFilePaths.Add(mzIdFilePath);
                        }
                        else
                        {
                            break;
                        }
                    }
                }
                else
                {
                    success = UpdateMzIdFile(remoteTransferFolder, dataPkgJob.Job, dataPkgJob.Dataset, searchedMzML, 0, sampleMetadata, out mzIdFilePath, out mzIdExistsRemotely);
                    if (success)
                    {
                        mzIdFilePaths.Add(mzIdFilePath);
                    }
                }

                if (!success)
                {
                    if (string.IsNullOrWhiteSpace(m_message))
                    {
                        LogError("UpdateMzIdFile returned false (unknown error)");
                    }
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception in UpdateMzIdFiles for job " + dataPkgJob.Job + ", dataset " + dataPkgJob.Dataset, ex);
                mzIdFilePaths = new List<string>();
                mzIdExistsRemotely = false;
                return false;
            }
        }

        /// <summary>
        /// Update a single .mzid.gz file to have the correct Accession value for FileFormat
        /// Also update attributes location and name for element SpectraData if we converted _dta.txt files to .mgf files
        /// Lastly, remove any empty ModificationParams elements
        /// </summary>
        /// <param name="remoteTransferFolder">Remote transfer folder</param>
        /// <param name="dataPkgJob">Data package job</param>
        /// <param name="dataPkgDataset">Data package dataset</param>
        /// <param name="searchedMzML">True if analysis job used a .mzML file (though we track .mzml.gz files with this class)</param>
        /// <param name="splitFastaResultID">For SplitFasta jobs, the part number being processed; 0 for non-SplitFasta jobs</param>
        /// <param name="sampleMetadata">Sample Metadata</param>
        /// <param name="mzIdFilePath">Output parameter: path to the .mzid.gz file being processed</param>
        /// <param name="mzIdExistsRemotely">Output parameter: true if the .mzid.gz file already exists in the remote transfer folder</param>
        /// <returns>True if success, false if an error</returns>
        private bool UpdateMzIdFile(
            string remoteTransferFolder,
            int dataPkgJob,
            string dataPkgDataset,
            bool searchedMzML,
            int splitFastaResultID,
            clsSampleMetadata sampleMetadata,
            out string mzIdFilePath,
            out bool mzIdExistsRemotely)
        {
            var readModAccession = false;
            var readingSpecificityRules = false;

            var attributeOverride = new Dictionary<string, string>();

            var elementCloseDepths = new Stack<int>();

            var eFileLocation = eMzIDXMLFileLocation.Header;
            var recentElements = new Queue<string>();

            try
            {
                var filePartText = string.Empty;

                if (splitFastaResultID > 0)
                {
                    filePartText = "_Part" + splitFastaResultID;
                }

                // First look for a job-specific version of the .mzid.gz file
                var sourceFileName = "Job" + dataPkgJob + "_" + dataPkgDataset + "_msgfplus" + filePartText + DOT_MZID_GZ;
                mzIdFilePath = Path.Combine(m_WorkDir, sourceFileName);

                if (!File.Exists(mzIdFilePath))
                {
                    // Job-specific version not found locally
                    // If the file already exists in the remote transfer folder, assume that it is up-to-date
                    if (FileExistsInTransferFolder(remoteTransferFolder, mzIdFilePath))
                    {
                        LogDebug("Skip updating the .mzid.gz file since already in the tranfer folder");
                        mzIdExistsRemotely = true;

                        StoreMzIdSampleInfo(mzIdFilePath, sampleMetadata);

                        return true;
                    }

                    // Look for one that simply starts with the dataset name
                    sourceFileName = dataPkgDataset + "_msgfplus" + filePartText + DOT_MZID_GZ;
                    mzIdFilePath = Path.Combine(m_WorkDir, sourceFileName);

                    if (!File.Exists(mzIdFilePath))
                    {
                        if (FileExistsInTransferFolder(remoteTransferFolder, mzIdFilePath))
                        {
                            LogDebug("Skip updating the .mzid.gz file since already in the tranfer folder");
                            mzIdExistsRemotely = true;

                            StoreMzIdSampleInfo(mzIdFilePath, sampleMetadata);

                            return true;
                        }

                        LogError("mzid.gz file not found for job " + dataPkgJob + ": " + sourceFileName);
                        mzIdExistsRemotely = false;
                        return false;
                    }
                }

                AddToListIfNew(mPreviousDatasetFilesToDelete, mzIdFilePath);

                var sourceMzidFile = new FileInfo(mzIdFilePath);
                var updatedMzidFile = new FileInfo(mzIdFilePath + ".tmp");

                var replaceOriginal = false;

                // Important: instantiate the XmlTextWriter using an instance of the UTF8Encoding class where the byte order mark (BOM) is not emitted
                // The ProteomeXchange import pipeline breaks if the .mzid files have the BOM at the start of the file
                // Note that the following Using command will not work if the .mzid file has an encoding string of <?xml version="1.0" encoding="Cp1252"?>
                // using (var xmlReader = new XmlTextReader(new FileStream(mzIdFilePath, FileMode.Open, FileAccess.Read)))
                // Thus, we instead first instantiate a streamreader using explicit encodings
                // Then instantiate the XmlTextReader

                using (var outFile = new FileStream(updatedMzidFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read))
                using (var zippedOutStream = new GZipStream(outFile, CompressionMode.Compress))
                using (var writer = new XmlTextWriter(zippedOutStream, new UTF8Encoding(false)))
                using (Stream unzippedStream = new GZipStream(new FileStream(mzIdFilePath, FileMode.Open, FileAccess.Read, FileShare.Read), CompressionMode.Decompress))
                using (var srSourceFile = new StreamReader(unzippedStream, Encoding.GetEncoding("ISO-8859-1")))
                using (var xmlReader = new XmlTextReader(srSourceFile))
                {
                    writer.Formatting = Formatting.Indented;
                    writer.Indentation = 2;

                    writer.WriteStartDocument();

                    while (xmlReader.Read())
                    {
                        switch (xmlReader.NodeType)
                        {
                            case XmlNodeType.Whitespace:
                                // Skip whitespace since the writer should be auto-formatting things
                                // writer.WriteWhitespace(xmlReader.Value)
                                break;

                            case XmlNodeType.Comment:
                                writer.WriteComment(xmlReader.Value);
                                break;

                            case XmlNodeType.Element:
                                // Start element

                                if (recentElements.Count > 10)
                                    recentElements.Dequeue();
                                recentElements.Enqueue("Element " + xmlReader.Name);

                                while (elementCloseDepths.Count > 0 && elementCloseDepths.Peek() > xmlReader.Depth)
                                {
                                    elementCloseDepths.Pop();

                                    writer.WriteEndElement();
                                }

                                eFileLocation = UpdateMZidXMLFileLocation(eFileLocation, xmlReader.Name);

                                var nodeWritten = false;
                                var skipNode = false;

                                attributeOverride.Clear();

                                switch (xmlReader.Name)
                                {
                                    case "SpectraData":
                                        if (searchedMzML)
                                        {
                                            // MSGF+ will list an .mzML file here
                                            // Although we upload .mzML.gz files, the .mzid.gz file needs to list the input file as .mzML
                                            // Thus, do not update the .mzid.gz file
                                        }
                                        else
                                        {
                                            // Override the location and name attributes for this node
                                            string spectraDataFilename;

                                            if (mCreateMGFFiles)
                                            {
                                                spectraDataFilename = dataPkgDataset + DOT_MGF;
                                            }
                                            else
                                            {
                                                spectraDataFilename = dataPkgDataset + "_dta.txt";
                                            }

                                            // The following statement intentionally uses a generic DMS_WorkDir path; do not use m_WorkDir
                                            attributeOverride.Add("location", @"C:\DMS_WorkDir\" + spectraDataFilename);
                                            attributeOverride.Add("name", spectraDataFilename);
                                        }
                                        break;

                                    case "ModificationParams":

                                        if (xmlReader.IsEmptyElement)
                                        {
                                            // Element is <ModificationParams />
                                            // Skip it
                                            skipNode = true;
                                        }
                                        break;

                                    case "FileFormat":

                                        if (eFileLocation == eMzIDXMLFileLocation.InputSpectraData && !searchedMzML)
                                        {
                                            // Override the accession and name attributes for this node

                                            // For .mzML files, the .mzID file should already have:
                                            //                         <cvParam accession="MS:1000584" cvRef="PSI-MS" name="mzML file"/>
                                            // For .mgf files,     use <cvParam accession="MS:1001062" cvRef="PSI-MS" name="Mascot MGF file"/>
                                            // For _dta.txt files, use <cvParam accession="MS:1001369" cvRef="PSI-MS" name="text file"/>

                                            string accession;
                                            string formatName;

                                            if (mCreateMGFFiles)
                                            {
                                                accession = "MS:1001062";
                                                formatName = "Mascot MGF file";
                                            }
                                            else
                                            {
                                                accession = "MS:1001369";
                                                formatName = "text file";
                                            }

                                            writer.WriteStartElement("FileFormat");
                                            writer.WriteStartElement("cvParam");

                                            writer.WriteAttributeString("accession", accession);
                                            writer.WriteAttributeString("cvRef", "PSI-MS");
                                            writer.WriteAttributeString("name", formatName);

                                            writer.WriteEndElement();  // cvParam
                                            writer.WriteEndElement();  // FileFormat

                                            skipNode = true;
                                        }
                                        break;

                                    case "SearchModification":
                                        if (eFileLocation == eMzIDXMLFileLocation.AnalysisProtocolCollection)
                                        {
                                            // The next cvParam entry that we read should have the Unimod accession
                                            readModAccession = true;
                                        }
                                        break;

                                    case "SpecificityRules":
                                        if (readModAccession)
                                        {
                                            readingSpecificityRules = true;
                                        }
                                        break;

                                    case "cvParam":
                                        if (readModAccession && !readingSpecificityRules)
                                        {
                                            var udtModInfo = ReadWriteCvParam(xmlReader, writer, elementCloseDepths);

                                            if (!string.IsNullOrEmpty(udtModInfo.Accession))
                                            {
                                                if (!mModificationsUsed.ContainsKey(udtModInfo.Accession))
                                                {
                                                    mModificationsUsed.Add(udtModInfo.Accession, udtModInfo);
                                                }

                                                if (!sampleMetadata.Modifications.ContainsKey(udtModInfo.Accession))
                                                {
                                                    sampleMetadata.Modifications.Add(udtModInfo.Accession, udtModInfo);
                                                }
                                            }

                                            nodeWritten = true;
                                        }
                                        break;
                                }

                                if (skipNode)
                                {
                                    if (xmlReader.NodeType != XmlNodeType.EndElement)
                                    {
                                        // Skip this element (and any children nodes enclosed in this elemnt)
                                        // Likely should not do this when xmlReader.NodeType is XmlNodeType.EndElement
                                        xmlReader.Skip();
                                    }
                                    replaceOriginal = true;
                                }
                                else if (!nodeWritten)
                                {
                                    // Copy this element from the source file to the target file

                                    writer.WriteStartElement(xmlReader.Name);

                                    if (xmlReader.HasAttributes)
                                    {
                                        xmlReader.MoveToFirstAttribute();
                                        do
                                        {
                                            if (attributeOverride.Count > 0 &&
                                                attributeOverride.TryGetValue(xmlReader.Name, out var overrideValue))
                                            {
                                                writer.WriteAttributeString(xmlReader.Name, overrideValue);
                                                replaceOriginal = true;
                                            }
                                            else
                                            {
                                                writer.WriteAttributeString(xmlReader.Name, xmlReader.Value);
                                            }
                                        } while (xmlReader.MoveToNextAttribute());

                                        elementCloseDepths.Push(xmlReader.Depth);
                                    }
                                    else if (xmlReader.IsEmptyElement)
                                    {
                                        writer.WriteEndElement();
                                    }
                                }
                                break;

                            case XmlNodeType.EndElement:

                                if (recentElements.Count > 10)
                                    recentElements.Dequeue();
                                recentElements.Enqueue("EndElement " + xmlReader.Name);

                                while (elementCloseDepths.Count > 0 && elementCloseDepths.Peek() > xmlReader.Depth + 1)
                                {
                                    elementCloseDepths.Pop();
                                    writer.WriteEndElement();
                                }

                                writer.WriteEndElement();

                                while (elementCloseDepths.Count > 0 && elementCloseDepths.Peek() > xmlReader.Depth)
                                {
                                    elementCloseDepths.Pop();
                                }

                                if (xmlReader.Name == "SearchModification")
                                {
                                    readModAccession = false;
                                }

                                if (xmlReader.Name == "SpecificityRules")
                                {
                                    readingSpecificityRules = false;
                                }
                                break;

                            case XmlNodeType.Text:

                                if (!string.IsNullOrEmpty(xmlReader.Value))
                                {
                                    if (recentElements.Count > 10)
                                        recentElements.Dequeue();
                                    if (xmlReader.Value.Length > 10)
                                    {
                                        recentElements.Enqueue(xmlReader.Value.Substring(0, 10));
                                    }
                                    else
                                    {
                                        recentElements.Enqueue(xmlReader.Value);
                                    }
                                }

                                writer.WriteString(xmlReader.Value);
                                break;
                        }
                    }

                    writer.WriteEndDocument();
                }

                StoreMzIdSampleInfo(mzIdFilePath, sampleMetadata);

                PRISM.clsProgRunner.GarbageCollectNow();

                mzIdExistsRemotely = false;
                if (!replaceOriginal)
                {
                    // Nothing was changed; delete the .tmp file
                    updatedMzidFile.Delete();
                    return true;
                }

                try
                {
                    // Update the date of the new .mzid.gz file to match the original file
                    updatedMzidFile.LastWriteTimeUtc = sourceMzidFile.LastWriteTimeUtc;

                    // Replace the original .mzid.gz file with the updated one
                    sourceMzidFile.Delete();

                    if (JobFileRenameRequired(dataPkgJob))
                    {
                        mzIdFilePath = Path.Combine(m_WorkDir, dataPkgDataset + "_Job" + dataPkgJob + "_msgfplus" + filePartText + DOT_MZID_GZ);
                    }
                    else
                    {
                        mzIdFilePath = Path.Combine(m_WorkDir, dataPkgDataset + "_msgfplus" + filePartText + DOT_MZID_GZ);
                    }

                    updatedMzidFile.MoveTo(mzIdFilePath);
                }
                catch (Exception ex)
                {
                    LogError("Exception replacing the original .mzid.gz file with the updated one for job " + dataPkgJob + ", dataset " + dataPkgDataset, ex);
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception in UpdateMzIdFile for job " + dataPkgJob + ", dataset " + dataPkgDataset, ex);

                var recentElementNames = string.Empty;
                foreach (var item in recentElements)
                {
                    if (string.IsNullOrEmpty(recentElementNames))
                    {
                        recentElementNames = string.Copy(item);
                    }
                    else
                    {
                        recentElementNames += "; " + item;
                    }
                }

                LogDebug(recentElementNames);

                mzIdFilePath = string.Empty;
                mzIdExistsRemotely = false;

                return false;
            }
        }

        private eMzIDXMLFileLocation UpdateMZidXMLFileLocation(eMzIDXMLFileLocation eFileLocation, string elementName)
        {
            switch (elementName)
            {
                case "SequenceCollection":
                    eFileLocation = eMzIDXMLFileLocation.SequenceCollection;
                    break;
                case "AnalysisCollection":
                    eFileLocation = eMzIDXMLFileLocation.AnalysisCollection;
                    break;
                case "AnalysisProtocolCollection":
                    eFileLocation = eMzIDXMLFileLocation.AnalysisProtocolCollection;
                    break;
                case "DataCollection":
                    eFileLocation = eMzIDXMLFileLocation.DataCollection;
                    break;
                case "Inputs":
                    eFileLocation = eMzIDXMLFileLocation.Inputs;
                    break;
                case "SearchDatabase":
                    eFileLocation = eMzIDXMLFileLocation.InputSearchDatabase;
                    break;
                case "SpectraData":
                    eFileLocation = eMzIDXMLFileLocation.InputSpectraData;
                    break;
                case "AnalysisData":
                    eFileLocation = eMzIDXMLFileLocation.AnalysisData;
                    break;
            }

            return eFileLocation;
        }

        /// <summary>
        /// If the CV param info is enclosed in square brackets, assure that it has exactly three commas
        /// </summary>
        /// <param name="cvParam"></param>
        /// <returns></returns>
        private string ValidateCV(string cvParam)
        {
            if (!cvParam.Trim().StartsWith("[") || !cvParam.Trim().EndsWith("]"))
                return cvParam;

            var valueParts = cvParam.Trim().TrimStart('[').TrimEnd(']').Split(',');
            if (valueParts.Length == 4)
                return cvParam;

            var updatedCV = new StringBuilder();
            updatedCV.Append('[');

            var itemsAppended = 0;
            foreach (var item in valueParts)
            {
                if (itemsAppended > 0)
                    updatedCV.Append(", ");

                updatedCV.Append(item.Trim());
                itemsAppended++;
            }

            while (itemsAppended < 4)
            {
                if (itemsAppended < 3)
                    updatedCV.Append(", xx_Undefined_xx");
                else
                    updatedCV.Append(", ");

                itemsAppended++;
            }

            updatedCV.Append(']');

            return updatedCV.ToString();
        }

        private void WriteConfigurationOption(XmlWriter writer, string KeyName, string Value)
        {
            writer.WriteStartElement("Option");
            writer.WriteElementString("Key", KeyName);
            writer.WriteElementString("Value", Value);
            writer.WriteEndElement();
        }

        /// <summary>
        /// Append a new header line to the .px file
        /// </summary>
        /// <param name="swPXFile"></param>
        /// <param name="type">Parameter type</param>
        /// <param name="value">Value for parameter</param>
        /// <param name="paramsWithCVs">Parameters that should have a CV</param>
        /// <remarks></remarks>
        private void WritePXHeader(TextWriter swPXFile, string type, string value, ICollection<string> paramsWithCVs)
        {
            WritePXHeader(swPXFile, type, value, new Dictionary<string, string>(), paramsWithCVs);
        }

        /// <summary>
        /// Append a new header line to the .px file
        /// </summary>
        /// <param name="swPXFile"></param>
        /// <param name="type">Parameter type</param>
        /// <param name="value">Value for parameter</param>
        /// <param name="templateParameters">Dictionary of parameters and values loaded from the template .px file</param>
        /// <param name="paramsWithCVs">Parameters that should have a CV</param>
        /// <param name="minimumValueLength">Minimum length for the parameter value</param>
        /// <remarks></remarks>
        private void WritePXHeader(
            TextWriter swPXFile,
            string type,
            string value,
            IReadOnlyDictionary<string, string> templateParameters,
            ICollection<string> paramsWithCVs,
            int minimumValueLength = 0)
        {
            if (templateParameters.TryGetValue(type, out var valueOverride))
            {
                if (!valueOverride.Equals("Auto-Defined", StringComparison.OrdinalIgnoreCase))
                {
                    if (type.Equals("tissue") && valueOverride.Contains("BRENDA"))
                    {
                        valueOverride = valueOverride.Replace("BRENDA", "BTO");
                    }

                    if (paramsWithCVs.Contains(type))
                    {
                        valueOverride = ValidateCV(valueOverride);
                    }

                    value = valueOverride;
                }
            }

            if (minimumValueLength > 0)
            {
                if (string.IsNullOrEmpty(value))
                {
                    value = "**** Value must be at least " + minimumValueLength + " characters long **** ";
                }

                while (value.Length < minimumValueLength)
                {
                    value += "__";
                }
            }

            WritePXLine(swPXFile, new List<string>
            {
                "MTD",
                type,
                value
            });
        }

        private void WritePXInstruments(TextWriter swPXFile, ICollection<string> paramsWithCVs)
        {
            foreach (var instrumentGroup in mInstrumentGroupsStored)
            {
                GetInstrumentAccession(instrumentGroup.Key, out var accession, out var description);

                if (instrumentGroup.Value.Contains("TSQ_2") && instrumentGroup.Value.Count == 1)
                {
                    // TSQ_1 is a TSQ Quantum Ultra
                    accession = "MS:1000751";
                    description = "TSQ Quantum Ultra";
                }

                var instrumentCV = GetInstrumentCv(accession, description);
                WritePXHeader(swPXFile, "instrument", instrumentCV, paramsWithCVs);
            }
        }

        private void WritePXLine(TextWriter swPXFile, IReadOnlyCollection<string> items)
        {
            if (items.Count > 0)
            {
                swPXFile.WriteLine(string.Join("\t", items));
            }
        }

        private void WritePXMods(TextWriter swPXFile, ICollection<string> paramsWithCVs)
        {
            if (mModificationsUsed.Count == 0)
            {
                var noPTMsCV = GetCVString("PRIDE", "PRIDE:0000398", "No PTMs are included in the dataset");
                WritePXHeader(swPXFile, "modification", noPTMsCV, paramsWithCVs);
            }
            else
            {
                // Write out each modification, for example, for Unimod:
                //   modification	[UNIMOD,UNIMOD:35,Oxidation,]
                // Or for PSI-mod
                //   modification	[MOD,MOD:00394,acetylated residue,]

                foreach (var item in mModificationsUsed)
                {
                    WritePXHeader(swPXFile, "modification", GetCVString(item.Value), paramsWithCVs);
                }
            }
        }

        private void WriteUserParam(XmlWriter writer, string name, string value)
        {
            writer.WriteStartElement("userParam");
            writer.WriteAttributeString("name", name);
            writer.WriteAttributeString("value", value);
            writer.WriteEndElement();
        }

        private void WriteCVParam(XmlWriter writer, string cvLabel, string accession, string name, string value)
        {
            writer.WriteStartElement("cvParam");
            writer.WriteAttributeString("cvLabel", cvLabel);
            writer.WriteAttributeString("accession", accession);
            writer.WriteAttributeString("name", name);
            writer.WriteAttributeString("value", value);
            writer.WriteEndElement();
        }

        private bool WriteXMLInstrumentInfo(XmlWriter oWriter, string instrumentGroup)
        {
            var instrumentDetailsAutoDefined = false;

            var isLCQ = false;
            var isLTQ = false;

            switch (instrumentGroup)
            {
                case "Orbitrap":
                case "VelosOrbi":
                case "QExactive":
                    instrumentDetailsAutoDefined = true;

                    WriteXMLInstrumentInfoESI(oWriter, "positive");

                    oWriter.WriteStartElement("analyzerList");
                    oWriter.WriteAttributeString("count", "2");

                    WriteXMLInstrumentInfoAnalyzer(oWriter, "MS", "MS:1000083", "radial ejection linear ion trap");
                    WriteXMLInstrumentInfoAnalyzer(oWriter, "MS", "MS:1000484", "orbitrap");

                    oWriter.WriteEndElement();   // analyzerList

                    WriteXMLInstrumentInfoDetector(oWriter, "MS", "MS:1000624", "inductive detector");
                    break;

                case "LCQ":
                    isLCQ = true;
                    break;

                case "LTQ":
                case "LTQ-ETD":
                case "LTQ-Prep":
                case "VelosPro":
                    isLTQ = true;
                    break;

                case "LTQ-FT":
                    instrumentDetailsAutoDefined = true;

                    WriteXMLInstrumentInfoESI(oWriter, "positive");

                    oWriter.WriteStartElement("analyzerList");
                    oWriter.WriteAttributeString("count", "2");

                    WriteXMLInstrumentInfoAnalyzer(oWriter, "MS", "MS:1000083", "radial ejection linear ion trap");
                    WriteXMLInstrumentInfoAnalyzer(oWriter, "MS", "MS:1000079", "fourier transform ion cyclotron resonance mass spectrometer");

                    oWriter.WriteEndElement();   // analyzerList

                    WriteXMLInstrumentInfoDetector(oWriter, "MS", "MS:1000624", "inductive detector");
                    break;

                case "Exactive":
                    instrumentDetailsAutoDefined = true;

                    WriteXMLInstrumentInfoESI(oWriter, "positive");

                    oWriter.WriteStartElement("analyzerList");
                    oWriter.WriteAttributeString("count", "1");

                    WriteXMLInstrumentInfoAnalyzer(oWriter, "MS", "MS:1000484", "orbitrap");

                    oWriter.WriteEndElement();   // analyzerList

                    WriteXMLInstrumentInfoDetector(oWriter, "MS", "MS:1000624", "inductive detector");
                    break;

                default:
                    if (instrumentGroup.StartsWith("LTQ"))
                    {
                        isLTQ = true;
                    }
                    else if (instrumentGroup.StartsWith("LCQ"))
                    {
                        isLCQ = true;
                    }
                    break;
            }

            if (isLTQ | isLCQ)
            {
                instrumentDetailsAutoDefined = true;

                WriteXMLInstrumentInfoESI(oWriter, "positive");

                oWriter.WriteStartElement("analyzerList");
                oWriter.WriteAttributeString("count", "1");

                if (isLCQ)
                {
                    WriteXMLInstrumentInfoAnalyzer(oWriter, "MS", "MS:1000082", "quadrupole ion trap");
                }
                else
                {
                    WriteXMLInstrumentInfoAnalyzer(oWriter, "MS", "MS:1000083", "radial ejection linear ion trap");
                }

                oWriter.WriteEndElement();   // analyzerList

                WriteXMLInstrumentInfoDetector(oWriter, "MS", "MS:1000347", "dynode");
            }

            return instrumentDetailsAutoDefined;
        }

        private void WriteXMLInstrumentInfoAnalyzer(XmlWriter oWriter, string cvLabel, string accession, string description)
        {
            oWriter.WriteStartElement("analyzer");
            WriteCVParam(oWriter, cvLabel, accession, description, string.Empty);
            oWriter.WriteEndElement();
        }

        private void WriteXMLInstrumentInfoDetector(XmlWriter oWriter, string cvLabel, string accession, string description)
        {
            oWriter.WriteStartElement("detector");
            WriteCVParam(oWriter, cvLabel, accession, description, string.Empty);
            oWriter.WriteEndElement();
        }

        private void WriteXMLInstrumentInfoESI(XmlWriter oWriter, string polarity)
        {
            if (string.IsNullOrEmpty(polarity))
                polarity = "positive";

            oWriter.WriteStartElement("source");
            WriteCVParam(oWriter, "MS", "MS:1000073", "electrospray ionization", string.Empty);
            WriteCVParam(oWriter, "MS", "MS:1000037", "polarity", polarity);
            oWriter.WriteEndElement();
        }

        #endregion

        #region "Event Handlers"

        private DateTime dtLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            if (DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15)
            {
                dtLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(m_WorkDir, PRIDEConverter_CONSOLE_OUTPUT));

                LogProgress("PRIDEConverter");
            }
        }

        private void mDTAtoMGF_ErrorEvent(string message)
        {
            LogError("Error from DTAtoMGF converter: " + mDTAtoMGF.GetErrorMessage());
        }

        private void mMSXmlCreator_LoopWaiting()
        {
            UpdateStatusFile();

            LogProgress("MSXmlCreator (PRIDEConverter)");
        }

        private void m_MyEMSLDatasetListInfo_FileDownloadedEvent(object sender, FileDownloadedEventArgs e)
        {
            if (e.UnzipRequired)
            {
                foreach (var unzippedFile in m_MyEMSLUtilities.MostRecentUnzippedFiles)
                {
                    AddToListIfNew(mPreviousDatasetFilesToDelete, unzippedFile.Value);
                }
            }

            AddToListIfNew(mPreviousDatasetFilesToDelete, Path.Combine(e.DownloadFolderPath, e.ArchivedFile.Filename));
        }

        #endregion
    }
}
