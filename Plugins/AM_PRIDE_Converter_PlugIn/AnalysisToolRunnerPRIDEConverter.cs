using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.JobConfig;
using MyEMSLReader;
using PHRPReader;
using PHRPReader.Data;
using PHRPReader.Reader;

namespace AnalysisManagerPRIDEConverterPlugIn
{
    // ReSharper disable CommentTypo

    // Ignore Spelling: acetylated, Bio, bool, const, Cv, dynode, electrospray, fasta, fourier, Frac, gzip, msg, msgf, msgfplus, musculus
    // Ignore Spelling: na, NTT, proteome, ProteomeXchange, proteomics, pubmed, Px, reportfile, roc, sapiens, sourcefile, spectrafile
    // Ignore Spelling: udt, Unimod, Unmarshaller, Xmx, Xpath, XpathPos, xtandem, yyyy-MM-dd
    // Ignore Spelling: amaZon, Bruker, Daltonics, Deca, Exactive, Exploris, Lumos, Orbitrap, SolariX, Velos

    // ReSharper restore CommentTypo

    /// <summary>
    /// Class for running PRIDEConverter
    /// </summary>
    /// <remarks>
    /// Although this class was originally created to prepare data for submission to PRIDE,
    /// we now primarily use it to submit data to ProteomeXchange
    /// </remarks>
    public class AnalysisToolRunnerPRIDEConverter : AnalysisToolRunnerBase
    {
        private const string DOT_MGF = AnalysisResources.DOT_MGF_EXTENSION;
        private const string DOT_MZID_GZ = ".mzid.gz";
        private const string DOT_MZML = AnalysisResources.DOT_MZML_EXTENSION;
        private const string DOT_MZML_GZ = AnalysisResources.DOT_MZML_EXTENSION + AnalysisResources.DOT_GZ_EXTENSION;

        private const string PRIDEConverter_CONSOLE_OUTPUT = "PRIDEConverter_ConsoleOutput.txt";

        /// <summary>
        /// Percent complete to report when the tool starts
        /// </summary>
        public const int PROGRESS_PCT_TOOL_RUNNER_STARTING = 20;

        private const int PROGRESS_PCT_SAVING_RESULTS = 95;
        private const int PROGRESS_PCT_COMPLETE = 99;

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
        private Dictionary<int, DataPackageJobInfo> mDataPackagePeptideHitJobs;

        private string mPrideConverterProgLoc = string.Empty;

        private readonly string mJavaProgLoc = string.Empty;
        private string mMSXmlGeneratorAppPath = string.Empty;

        // [Obsolete("No longer used")]
        // private bool mCreateMSGFReportFilesOnly;

        private bool mCreateMGFFiles;

        // [Obsolete("No longer used")]
        // private bool mCreatePrideXMLFiles;

        // [Obsolete("No longer used")]
        // private bool mIncludePepXMLFiles;

        private bool mProcessMzIdFiles;

        private string mCacheDirectoryPath = string.Empty;
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
        [Obsolete("No longer used")]
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
        private Dictionary<string, PXFileInfoBase> mPxMasterFileList;

        /// <summary>
        /// Px result files
        /// </summary>
        /// <remarks>
        /// Keys are PXFileIDs
        /// Values contain info on each file, including the PXFileType and the FileIDs that map to this file (empty list if no mapped files)
        /// </remarks>
        private Dictionary<int, PXFileInfo> mPxResultFiles;

        [Obsolete("No longer used")]
        private FilterThresholds mFilterThresholdsUsed;

        /// <summary>
        /// Instrument group names
        /// </summary>
        /// <remarks>
        /// Keys are instrument group names
        /// Values are the specific instrument names in the instrument group
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
        private Dictionary<string, SampleMetadata.CvParamInfo> mModificationsUsed;

        /// <summary>
        /// Sample info for each mzid.gz file
        /// (instantiated with a case-insensitive comparer)
        /// </summary>
        /// <remarks>
        /// Keys are mzid.gz file names
        /// Values are the sample info for the file
        /// </remarks>
        private Dictionary<string, SampleMetadata> mMzIdSampleInfo;

        /// <summary>
        /// _dta.txt file stats
        /// </summary>
        /// <remarks>
        /// Keys are _dta.txt file names
        /// Values contain info on each file
        /// </remarks>
        private Dictionary<string, PXFileInfoBase> mCDTAFileStats;

        /// <summary>
        /// MzML / MzXML file creator
        /// </summary>
        private AnalysisManagerMsXmlGenPlugIn.MSXMLCreator mMSXmlCreator;

        /// <summary>
        /// Program runner
        /// </summary>
        private RunDosProgram mCmdRunner;

        [Obsolete("No longer used")]
        private struct FilterThresholds
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

        private struct PseudoMSGFData
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

            /// <summary>
            /// Show Result ID and peptide
            /// </summary>
            public readonly override string ToString()
            {
                if (string.IsNullOrWhiteSpace(Peptide))
                    return "ResultID " + ResultID;

                return "ResultID " + ResultID + ": " + Peptide;
            }
        }

        private enum MSGFReportXMLFileLocations
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

        private enum MzidXMLFileLocations
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

                if (mDebugLevel > 4)
                {
                    LogDebug("AnalysisToolRunnerPRIDEConverter.RunTool(): Enter");
                }

                // Verify that program files exist
                if (!DefineProgramPaths())
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the PRIDE Converter version info in the database
                if (!StoreToolVersionInfo())
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining PRIDE Converter version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mConsoleOutputErrorMsg = string.Empty;

                mCacheDirectoryPath = mJobParams.GetJobParameter("CacheFolderPath", string.Empty);

                if (string.IsNullOrWhiteSpace(mCacheDirectoryPath))
                {
                    mCacheDirectoryPath = mJobParams.GetJobParameter("CacheDirectoryPath", AnalysisResourcesPRIDEConverter.DEFAULT_CACHE_DIRECTORY_PATH);
                }

                LogMessage("Running PRIDEConverter");

                // Initialize dataPackageDatasets
                if (!LoadDataPackageDatasetInfo(out var dataPackageDatasets, out var errorMessage, false))
                {
                    const string msg = "Error loading data package dataset info";

                    if (string.IsNullOrWhiteSpace(errorMessage))
                    {
                        LogError(string.Format("{0}: AnalysisToolRunnerBase.LoadDataPackageDatasetInfo returned false", msg));
                        mMessage = msg;
                    }
                    else
                    {
                        LogError("{0}: {1}", msg, errorMessage);
                        mMessage = errorMessage;
                    }

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Initialize mDataPackagePeptideHitJobs
                if (!LookupDataPackagePeptideHitJobs())
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Monitor the FileDownloaded event in this class
                mMyEMSLUtilities.FileDownloaded += MyEMSLDatasetListInfo_FileDownloadedEvent;

                // The analysisResults object is used to copy files to/from this computer
                var analysisResults = new AnalysisResults(mMgrParams, mJobParams);

                // Assure that the remote transfer directory exists
                var remoteTransferDirectory = CreateRemoteTransferDirectory(analysisResults, mCacheDirectoryPath);

                try
                {
                    // Create the remote Transfer Directory
                    analysisResults.CreateDirectoryWithRetry(remoteTransferDirectory, maxRetryCount: 5, retryHoldoffSeconds: 20, increaseHoldoffOnEachRetry: true);
                }
                catch (Exception ex)
                {
                    // Folder creation error
                    LogError("Exception creating transfer directory folder", ex);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Read the PX_Submission_Template.px file
                var templateParameters = ReadTemplatePXSubmissionFile();

                var jobFailureCount = ProcessJobs(analysisResults, remoteTransferDirectory, templateParameters, dataPackageDatasets);

                // Create the PX Submission file
                var success = CreatePXSubmissionFile(templateParameters);

                mProgress = PROGRESS_PCT_COMPLETE;
                mStatusTools.UpdateAndWrite(mProgress);

                if (success)
                {
                    if (mDebugLevel >= 3)
                    {
                        LogDebug("PRIDEConverter Complete");
                    }
                }

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                PRISM.AppUtils.GarbageCollectNow();

                if (!success || jobFailureCount > 0)
                {
                    // Something went wrong
                    // In order to help diagnose things, move the output files into the results directory,
                    // archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                DefineFilesToSkipTransfer();

                var copySuccess = CopyResultsToTransferDirectory(mCacheDirectoryPath);

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
        /// <param name="remoteTransferDirectory"></param>
        /// <param name="templateParameters"></param>
        /// <param name="dataPackageDatasets">Datasets in the data package (keys are DatasetID)</param>
        private int ProcessJobs(
            AnalysisResults analysisResults,
            string remoteTransferDirectory,
            IReadOnlyDictionary<string, string> templateParameters,
            IReadOnlyDictionary<int, DataPackageDatasetInfo> dataPackageDatasets)
        {
            var jobsProcessed = 0;
            var jobFailureCount = 0;

            try
            {
                // Initialize the class-wide variables
                InitializeOptions();

                // Extract the dataset raw file paths
                var datasetRawFilePaths = ExtractPackedJobParameterDictionary(AnalysisResources.JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS);

                // Process each job in mDataPackagePeptideHitJobs
                // Sort the jobs by dataset so that we can use the same .mzML file for datasets with multiple jobs
                var linqJobsSortedByDataset = (from item in mDataPackagePeptideHitJobs orderby item.Value.Dataset, SortPreference(item.Value.Tool) select item);

                var assumeInstrumentDataUnpurged = mJobParams.GetJobParameter("AssumeInstrumentDataUnpurged", true);

                const bool continueOnError = true;
                const int maxErrorCount = 10;
                var lastLogTime = DateTime.UtcNow;

                // This dictionary tracks the datasets that have been processed
                // Keys are dataset ID, values are dataset name
                var datasetsProcessed = new Dictionary<int, string>();

                foreach (var jobInfo in linqJobsSortedByDataset)
                {
                    var currentJobInfo = jobInfo.Value;

                    mStatusTools.CurrentOperation = "Processing job " + currentJobInfo.Job + ", dataset " + currentJobInfo.Dataset;

                    Console.WriteLine();
                    LogDebug(string.Format("{0}: {1}", jobsProcessed + 1, mStatusTools.CurrentOperation), 10);

                    var result = ProcessJob(
                        jobInfo, analysisResults, dataPackageDatasets,
                        remoteTransferDirectory, datasetRawFilePaths,
                        templateParameters, assumeInstrumentDataUnpurged);

                    if (result != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        jobFailureCount++;

                        if (!continueOnError || jobFailureCount > maxErrorCount)
                            break;
                    }

                    if (!datasetsProcessed.ContainsKey(currentJobInfo.DatasetID))
                    {
                        datasetsProcessed.Add(currentJobInfo.DatasetID, currentJobInfo.Dataset);
                    }

                    jobsProcessed++;
                    mProgress = ComputeIncrementalProgress(PROGRESS_PCT_TOOL_RUNNER_STARTING, PROGRESS_PCT_SAVING_RESULTS, jobsProcessed,
                        mDataPackagePeptideHitJobs.Count);
                    mStatusTools.UpdateAndWrite(mProgress);

                    if (DateTime.UtcNow.Subtract(lastLogTime).TotalMinutes >= 5 || mDebugLevel >= 2)
                    {
                        lastLogTime = DateTime.UtcNow;
                        LogDebug(" ... processed " + jobsProcessed + " / " + mDataPackagePeptideHitJobs.Count + " jobs");
                    }
                }

                TransferPreviousDatasetFiles(analysisResults, remoteTransferDirectory);

                // Look for datasets associated with the data package that have no PeptideHit jobs
                // Create fake PeptideHit jobs in the .px file to alert the user of the missing jobs

                foreach (var datasetInfo in dataPackageDatasets)
                {
                    var datasetId = datasetInfo.Key;
                    var datasetName = datasetInfo.Value.Dataset;

                    if (datasetsProcessed.ContainsKey(datasetId))
                        continue;

                    mStatusTools.CurrentOperation = "Adding dataset " + datasetName + " (no associated PeptideHit job)";

                    Console.WriteLine();
                    LogDebug(mStatusTools.CurrentOperation, 10);

                    AddPlaceholderDatasetEntry(datasetInfo.Value);
                }

                // If we were still unable to delete some files, we want to make sure that they don't end up in the results folder
                foreach (var fileToDelete in mPreviousDatasetFilesToDelete)
                {
                    mJobParams.AddResultFileToSkip(fileToDelete);
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
            if (tool.StartsWith("msgfplus", StringComparison.OrdinalIgnoreCase))
                return 0;

            if (tool.StartsWith("xtandem", StringComparison.OrdinalIgnoreCase))
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

        private void AddPlaceholderDatasetEntry(DataPackageDatasetInfo datasetInfo)
        {
            AddExperimentTissueId(datasetInfo.Experiment_Tissue_ID, datasetInfo.Experiment_Tissue_Name);

            AddNEWTInfo(datasetInfo.Experiment_NEWT_ID, datasetInfo.Experiment_NEWT_Name);

            // Store the instrument group and instrument name
            StoreInstrumentInfo(datasetInfo);

            var datasetRawFilePath = Path.Combine(datasetInfo.DatasetDirectoryPath, datasetInfo.Dataset + ".raw");

            var dataPkgJob = AnalysisResources.GetPseudoDataPackageJobInfo(datasetInfo);

            var rawFileID = AddPxFileToMasterList(datasetRawFilePath, dataPkgJob);

            AddPxResultFile(rawFileID, PXFileInfoBase.PXFileTypes.Raw, datasetRawFilePath, dataPkgJob);
        }

        private int AddPxFileToMasterList(string filePath, DataPackageJobInfo dataPkgJob)
        {
            var file = new FileInfo(filePath);

            if (mPxMasterFileList.TryGetValue(file.Name, out var pxFileInfo))
            {
                // File already exists
                return pxFileInfo.FileID;
            }

            var filename = CheckFilenameCase(file, dataPkgJob.Dataset);

            pxFileInfo = new PXFileInfoBase(filename, dataPkgJob)
            {
                FileID = mPxMasterFileList.Count + 1
            };

            if (file.Exists)
            {
                pxFileInfo.Length = file.Length;
                pxFileInfo.MD5Hash = string.Empty;      // Don't compute the hash; it's not needed
            }
            else
            {
                pxFileInfo.Length = 0;
                pxFileInfo.MD5Hash = string.Empty;
            }

            mPxMasterFileList.Add(file.Name, pxFileInfo);

            return pxFileInfo.FileID;
        }

        private bool AddPxResultFile(int fileId, PXFileInfoBase.PXFileTypes eFileType, string filePath, DataPackageJobInfo dataPkgJob)
        {
            var file = new FileInfo(filePath);

            if (mPxResultFiles.TryGetValue(fileId, out _))
            {
                // File already defined in the mapping list
                return true;
            }

            if (!mPxMasterFileList.TryGetValue(file.Name, out var masterPXFileInfo))
            {
                // File not found in mPxMasterFileList, we cannot add the mapping
                LogError("File " + file.Name + " not found in mPxMasterFileList; unable to add to mPxResultFiles");
                return false;
            }

            if (masterPXFileInfo.FileID != fileId)
            {
                var msg = "FileID mismatch for " + file.Name;
                LogError("{0}: mPxMasterFileList.FileID = {1} vs. FileID {2} passed into AddPxFileToMapping",
                    msg, masterPXFileInfo.FileID, fileId);

                mMessage = msg;
                return false;
            }

            var filename = CheckFilenameCase(file, dataPkgJob.Dataset);

            var pxFileInfo = new PXFileInfo(filename, dataPkgJob);
            pxFileInfo.Update(masterPXFileInfo);
            pxFileInfo.PXFileType = eFileType;

            mPxResultFiles.Add(fileId, pxFileInfo);

            return true;
        }

        /// <summary>
        /// Adds value to listToUpdate only if the value is not yet present in the list
        /// </summary>
        /// <param name="listToUpdate"></param>
        /// <param name="value"></param>
        private void AddToListIfNew(ICollection<string> listToUpdate, string value)
        {
            if (!listToUpdate.Contains(value))
            {
                listToUpdate.Add(value);
            }
        }

        private bool AppendToPXFileInfo(DataPackageJobInfo dataPkgJob, IReadOnlyDictionary<string, string> datasetRawFilePaths,
            ResultFileContainer resultFiles)
        {
            // Add the files to be submitted to ProteomeXchange to the master file list
            // In addition, append new mappings to the ProteomeXchange mapping list

            var prideXMLFileId = 0;

            if (!string.IsNullOrEmpty(resultFiles.PrideXmlFilePath))
            {
                AddToListIfNew(mPreviousDatasetFilesToCopy, resultFiles.PrideXmlFilePath);

                prideXMLFileId = AddPxFileToMasterList(resultFiles.PrideXmlFilePath, dataPkgJob);

                if (!AddPxResultFile(prideXMLFileId, PXFileInfoBase.PXFileTypes.Result, resultFiles.PrideXmlFilePath, dataPkgJob))
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

                    if (!AddPxResultFile(rawFileID, PXFileInfoBase.PXFileTypes.Raw, datasetRawFilePath, dataPkgJob))
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

                if (!AddPxResultFile(peakFileId, PXFileInfoBase.PXFileTypes.Peak, resultFiles.MGFFilePath, dataPkgJob))
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
                var success = AddMzidOrPepXmlFileToPX(dataPkgJob, mzIdResultFile, PXFileInfoBase.PXFileTypes.ResultMzId, prideXMLFileId,
                    rawFileID, peakFileId);

                if (!success)
                    return false;
            }

            if (!string.IsNullOrWhiteSpace(resultFiles.PepXMLFile))
            {
                var success = AddMzidOrPepXmlFileToPX(dataPkgJob, resultFiles.PepXMLFile, PXFileInfoBase.PXFileTypes.Search, prideXMLFileId,
                    rawFileID, peakFileId);

                if (!success)
                    return false;
            }

            return true;
        }

        private bool AddMzidOrPepXmlFileToPX(DataPackageJobInfo dataPkgJob, string resultFilePath, PXFileInfoBase.PXFileTypes ePxFileType,
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

        private string CheckFilenameCase(FileSystemInfo file, string dataset)
        {
            var filename = file.Name;

            if (!string.IsNullOrEmpty(file.Extension))
            {
                var fileBaseName = Path.GetFileNameWithoutExtension(file.Name);

                if (fileBaseName.StartsWith(dataset, StringComparison.OrdinalIgnoreCase))
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

                if (file.Extension.Equals(DOT_MZML, StringComparison.OrdinalIgnoreCase))
                {
                    filename = fileBaseName + DOT_MZML;
                }
                else if (file.Extension.Equals(DOT_MZML_GZ, StringComparison.OrdinalIgnoreCase))
                {
                    filename = fileBaseName + DOT_MZML_GZ;
                }
                else
                {
                    filename = fileBaseName + file.Extension.ToLower();
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
        /// <param name="mgfFilePath">Output: path of the newly created .mgf file</param>
        /// <returns>True if success, false if an error</returns>
        private bool ConvertCDTAToMGF(DataPackageJobInfo dataPkgJob, out string mgfFilePath)
        {
            mgfFilePath = string.Empty;

            try
            {
                var cdtaUtilities = new CDTAUtilities();
                RegisterEvents(cdtaUtilities);

                const bool combine2And3PlusCharges = false;
                const int maximumIonsPer100MzInterval = 40;
                const bool createIndexFile = false;

                // Convert the _dta.txt file for this data package job
                var cdtaFile = new FileInfo(Path.Combine(mWorkDir, dataPkgJob.Dataset + AnalysisResources.CDTA_EXTENSION));

                // Compute the MD5 hash for this _dta.txt file
                var md5Hash = PRISM.HashUtilities.ComputeFileHashMD5(cdtaFile.FullName);

                // Make sure this is either a new _dta.txt file or identical to a previous one
                // Abort processing if the job list contains multiple jobs for the same dataset but those jobs used different _dta.txt files
                // However, if one of the jobs is SEQUEST and one is MS-GF+, preferentially use the _dta.txt file from the MS-GF+ job

                if (mCDTAFileStats.TryGetValue(cdtaFile.Name, out var existingFileInfo))
                {
                    if (existingFileInfo.JobInfo.Tool.StartsWith("msgf", StringComparison.OrdinalIgnoreCase))
                    {
                        // Existing job found, but it's a MS-GF+ job (which is fully supported by PRIDE)
                        // Just use the existing .mgf file
                        return true;
                    }

                    if (cdtaFile.Length != existingFileInfo.Length)
                    {
                        var msg = string.Format(
                            "Dataset {0} has multiple jobs in this data package, and those jobs used different _dta.txt files; this is not supported",
                            dataPkgJob.Dataset);

                        LogError("{0}: file size mismatch of {1} for job {2} vs. {3} for job {4}",
                            msg, cdtaFile.Length, dataPkgJob.Job, existingFileInfo.Length, existingFileInfo.JobInfo.Job);

                        mMessage = msg;
                        return false;
                    }

                    if (md5Hash != existingFileInfo.MD5Hash)
                    {
                        var msg = string.Format(
                            "Dataset {0} has multiple jobs in this data package, and those jobs used different _dta.txt files; this is not supported",
                            dataPkgJob.Dataset);

                        LogError("{0}: MD5 hash mismatch of {1} for job {2} vs. {3} for job {4}",
                            msg, md5Hash, dataPkgJob.Job, existingFileInfo.MD5Hash, existingFileInfo.JobInfo.Job);

                        mMessage = msg;
                        return false;
                    }

                    // The files match; no point in making a new .mgf file
                    return true;
                }

                var filename = CheckFilenameCase(cdtaFile, dataPkgJob.Dataset);

                var fileInfo = new PXFileInfoBase(filename, dataPkgJob)
                {
                    // File ID doesn't matter; just use 0
                    FileID = 0,
                    Length = cdtaFile.Length,
                    MD5Hash = md5Hash
                };

                mCDTAFileStats.Add(cdtaFile.Name, fileInfo);

                var success = cdtaUtilities.ConvertCDTAToMGF(cdtaFile, dataPkgJob.Dataset, combine2And3PlusCharges, maximumIonsPer100MzInterval, createIndexFile);

                if (!success)
                {
                    LogError("Error converting " + cdtaFile.Name + " to a .mgf file for job " + dataPkgJob.Job);
                    return false;
                }

                // Delete the _dta.txt file
                try
                {
                    cdtaFile.Delete();
                }
                catch (Exception ex)
                {
                    LogWarning("Unable to delete the _dta.txt file after successfully converting it to .mgf: " + ex.Message);
                }

                PRISM.AppUtils.GarbageCollectNow();

                var newMGFFile = new FileInfo(Path.Combine(mWorkDir, dataPkgJob.Dataset + DOT_MGF));

                if (!newMGFFile.Exists)
                {
                    // MGF file was not created
                    LogError("A .mgf file was not created for the _dta.txt file for job " + dataPkgJob.Job);
                    return false;
                }

                mgfFilePath = newMGFFile.FullName;

                return true;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in ConvertCDTAToMGF";
                LogError(mMessage, ex);
                return false;
            }
        }

        /// <summary>
        /// Copy failed results to the archive folder
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory()
        {
            // Make sure the PRIDEConverter console output file is retained
            mJobParams.RemoveResultFileToSkip(PRIDEConverter_CONSOLE_OUTPUT);

            // Skip the .mgf files; no need to put them in the FailedResults folder
            mJobParams.AddResultFileExtensionToSkip(DOT_MGF);

            base.CopyFailedResultsToArchiveDirectory();
        }

        /// <summary>
        /// Counts the number of items of type eFileType in mPxResultFiles
        /// </summary>
        /// <param name="eFileType"></param>
        private int CountResultFilesByType(PXFileInfoBase.PXFileTypes eFileType)
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
        [Obsolete("No longer used")]
        private bool CreateMzXMLFileIfMissing(string dataset, AnalysisResults analysisResults,
            IReadOnlyDictionary<string, string> datasetRawFilePaths)
        {
            try
            {
                // Look in mWorkDir for the .mzXML file for this dataset
                var mzXmlFilePathLocal = new FileInfo(Path.Combine(mWorkDir, dataset + AnalysisResources.DOT_MZXML_EXTENSION));

                if (mzXmlFilePathLocal.Exists)
                {
                    if (!mPreviousDatasetFilesToDelete.Contains(mzXmlFilePathLocal.FullName))
                    {
                        AddToListIfNew(mPreviousDatasetFilesToDelete, mzXmlFilePathLocal.FullName);
                    }
                    return true;
                }

                // .mzXML file not found
                // Look for a StoragePathInfo file
                var mzXmlStoragePathFile = mzXmlFilePathLocal.FullName + AnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX;

                string destinationPath;
                bool success;

                if (File.Exists(mzXmlStoragePathFile))
                {
                    success = RetrieveStoragePathInfoTargetFile(mzXmlStoragePathFile, analysisResults, out destinationPath);

                    if (success)
                    {
                        AddToListIfNew(mPreviousDatasetFilesToDelete, destinationPath);
                        return true;
                    }
                }

                // Need to create the .mzXML file

                var datasetYearQuarterByDataset =
                    ExtractPackedJobParameterDictionary(AnalysisResourcesPRIDEConverter.JOB_PARAM_DICTIONARY_DATASET_STORAGE_YEAR_QUARTER);

                if (!datasetRawFilePaths.ContainsKey(dataset))
                {
                    LogError("Dataset " + dataset + " not found in job parameter " +
                        AnalysisResources.JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS +
                        "; unable to create the missing .mzXML file");
                    return false;
                }

                mJobParams.AddResultFileToSkip("MSConvert_ConsoleOutput.txt");

                mMSXmlCreator = new AnalysisManagerMsXmlGenPlugIn.MSXMLCreator(mMSXmlGeneratorAppPath, mWorkDir,
                                                                                  dataset, mDebugLevel, mJobParams);
                RegisterEvents(mMSXmlCreator);
                mMSXmlCreator.LoopWaiting += MSXmlCreator_LoopWaiting;

                mMSXmlCreator.UpdateDatasetName(dataset);

                // Make sure the dataset file is present in the working directory
                // Copy it locally if necessary

                var datasetFilePathRemote = datasetRawFilePaths[dataset];

                if (string.IsNullOrWhiteSpace(datasetFilePathRemote))
                {
                    LogError("Dataset " + dataset + " has an empty value for the instrument file path in " +
                        AnalysisResources.JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS +
                        "; unable to create the missing .mzXML file");
                    return false;
                }
                var datasetFileIsAFolder = Directory.Exists(datasetFilePathRemote);

                var datasetFilePathLocal = Path.Combine(mWorkDir, Path.GetFileName(datasetFilePathRemote));

                if (datasetFileIsAFolder)
                {
                    // Confirm that the dataset directory exists in the working directory

                    if (!Directory.Exists(datasetFilePathLocal))
                    {
                        // Directory not found; look for a storage path info file
                        if (File.Exists(datasetFilePathLocal + AnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX))
                        {
                            RetrieveStoragePathInfoTargetFile(datasetFilePathLocal + AnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX,
                                analysisResults, IsFolder: true, destinationPath: out destinationPath);
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
                        if (File.Exists(datasetFilePathLocal + AnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX))
                        {
                            RetrieveStoragePathInfoTargetFile(datasetFilePathLocal + AnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX,
                                analysisResults, out destinationPath);
                            AddToListIfNew(mPreviousDatasetFilesToDelete, destinationPath);
                        }
                        else
                        {
                            // Copy the dataset file locally
                            analysisResults.CopyFileWithRetry(datasetFilePathRemote, datasetFilePathLocal, overwrite: true);
                            AddToListIfNew(mPreviousDatasetFilesToDelete, datasetFilePathLocal);
                        }
                    }
                    mJobParams.AddResultFileToSkip(Path.GetFileName(datasetFilePathLocal));
                }

                success = mMSXmlCreator.CreateMZXMLFile();

                if (!success && string.IsNullOrEmpty(mMessage))
                {
                    mMessage = mMSXmlCreator.ErrorMessage;

                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Unknown error creating the mzXML file for dataset " + dataset;
                    }
                    else if (!mMessage.Contains(dataset))
                    {
                        mMessage += "; dataset " + dataset;
                    }
                    LogError(mMessage);
                }

                if (!success)
                    return false;

                mzXmlFilePathLocal.Refresh();

                if (mzXmlFilePathLocal.Exists)
                {
                    AddToListIfNew(mPreviousDatasetFilesToDelete, mzXmlFilePathLocal.FullName);
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

                CopyMzXMLFileToServerCache(mzXmlFilePathLocal.FullName, datasetYearQuarter, msXmlGeneratorName, purgeOldFilesIfNeeded: true);

                mJobParams.AddResultFileToSkip(Path.GetFileName(mzXmlFilePathLocal.FullName + Global.SERVER_CACHE_HASHCHECK_FILE_SUFFIX));

                PRISM.ProgRunner.GarbageCollectNow();

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

        [Obsolete("No longer used")]
        private string CreatePseudoMSGFFileUsingPHRPReader(int job, string dataset, FilterThresholds filterThresholds,
            IDictionary<string, List<PseudoMSGFData>> pseudoMSGFData)
        {
            const int MSGF_SPEC_EVALUE_NOT_DEFINED = 10;
            const int PVALUE_NOT_DEFINED = 10;

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

                // The keys in each of bestMatchByScan and bestMatchByScanScoreValues are scan numbers
                // The value for bestMatchByScan is a KeyValue pair where the key is the score for this match
                var bestMatchByScan = new Dictionary<int, KeyValuePair<double, string>>();
                var bestMatchByScanScoreValues = new Dictionary<int, PseudoMSGFData>();

                var mzXMLFilename = dataset + ".mzXML";

                // Determine the correct capitalization for the mzXML file
                var workDir = new DirectoryInfo(mWorkDir);
                var matchingFiles = workDir.GetFiles(mzXMLFilename);

                if (matchingFiles.Length > 0)
                {
                    mzXMLFilename = matchingFiles[0].Name;
                }
                // else
                // mzXML file not found; don't worry about this right now (it's possible that CreateMSGFReportFilesOnly is true)

                var synopsisFileName = ReaderFactory.GetPHRPSynopsisFileName(dataPkgJob.PeptideHitResultType, dataPkgJob.Dataset);

                var synopsisFilePath = Path.Combine(mWorkDir, synopsisFileName);

                if (!File.Exists(synopsisFilePath))
                {
                    var synopsisFilePathAlt = ReaderFactory.AutoSwitchToLegacyMSGFDBIfRequired(synopsisFilePath, "Dataset_msgfdb.txt");

                    if (File.Exists(synopsisFilePathAlt))
                    {
                        synopsisFilePath = synopsisFilePathAlt;
                    }
                }

                // Check whether PHRP files with a prefix of "Job12345_" exist
                // This prefix is added by RetrieveDataPackagePeptideHitJobPHRPFiles if multiple peptide_hit jobs are included for the same dataset
                var synopsisFilePathWithJob = Path.Combine(mWorkDir, "Job" + dataPkgJob.Job + "_" + synopsisFileName);

                if (File.Exists(synopsisFilePathWithJob))
                {
                    synopsisFilePath = synopsisFilePathWithJob;
                }
                else if (!File.Exists(synopsisFilePath))
                {
                    var synopsisFilePathAlt = ReaderFactory.AutoSwitchToLegacyMSGFDBIfRequired(synopsisFilePathWithJob, "Dataset_msgfdb.txt");

                    if (File.Exists(synopsisFilePathAlt))
                    {
                        synopsisFilePath = synopsisFilePathAlt;
                    }
                }

                using (var reader = new ReaderFactory(synopsisFilePath, dataPkgJob.PeptideHitResultType, true, true))
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
                        var pValue = (double)PVALUE_NOT_DEFINED;
                        var scoreForCurrentMatch = 100.0;

                        // Determine MSGFSpecEValue; store 10 if we don't find a valid number
                        if (!double.TryParse(reader.CurrentPSM.MSGFSpecEValue, out var msgfSpecEValue))
                        {
                            msgfSpecEValue = MSGF_SPEC_EVALUE_NOT_DEFINED;
                        }

                        switch (dataPkgJob.PeptideHitResultType)
                        {
                            case PeptideHitResultTypes.Sequest:
                                if (msgfSpecEValue < MSGF_SPEC_EVALUE_NOT_DEFINED)
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
                                        scoreForCurrentMatch = 1000 - reader.CurrentPSM.GetScoreDbl(SequestSynFileReader.GetColumnNameByID(SequestSynopsisFileColumns.XCorr), 1);
                                    }
                                }
                                break;

                            case PeptideHitResultTypes.XTandem:
                                if (msgfSpecEValue < MSGF_SPEC_EVALUE_NOT_DEFINED)
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

                                        // Peptide_Expectation_Value_LogE
                                        scoreForCurrentMatch = 1000 + reader.CurrentPSM.GetScoreDbl(XTandemSynFileReader.GetColumnNameByID(XTandemSynFileColumns.EValue), 1);
                                    }
                                }
                                break;

                            case PeptideHitResultTypes.Inspect:
                                pValue = reader.CurrentPSM.GetScoreDbl(InspectSynFileReader.GetColumnNameByID(InspectSynFileColumns.PValue), PVALUE_NOT_DEFINED);

                                if (msgfSpecEValue < MSGF_SPEC_EVALUE_NOT_DEFINED)
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
                                        scoreForCurrentMatch = 1000 - (reader.CurrentPSM.GetScoreDbl(InspectSynFileReader.GetColumnNameByID(InspectSynFileColumns.TotalPRMScore), 1));
                                    }
                                }
                                break;

                            case PeptideHitResultTypes.MSGFPlus:
                                fdr = reader.CurrentPSM.GetScoreDbl(MSGFPlusSynFileReader.GetColumnNameByID(MSGFPlusSynFileColumns.QValue), -1);

                                if (fdr > -1)
                                {
                                    fdrValuesArePresent = true;
                                }

                                pepFDR = reader.CurrentPSM.GetScoreDbl(MSGFPlusSynFileReader.GetColumnNameByID(MSGFPlusSynFileColumns.PepQValue), -1);

                                if (pepFDR > -1)
                                {
                                    pepFDRValuesArePresent = true;
                                }

                                pValue = reader.CurrentPSM.GetScoreDbl(MSGFPlusSynFileReader.GetColumnNameByID(MSGFPlusSynFileColumns.EValue), PVALUE_NOT_DEFINED);
                                scoreForCurrentMatch = msgfSpecEValue;
                                break;
                        }

                        if (filterThresholds.UseMSGFSpecEValue)
                        {
                            if (msgfSpecEValue > filterThresholds.MSGFSpecEValueThreshold)
                            {
                                validPSM = false;
                            }
                            thresholdChecked = true;

                            if (!mFilterThresholdsUsed.UseMSGFSpecEValue)
                            {
                                mFilterThresholdsUsed.UseMSGFSpecEValue = true;
                                mFilterThresholdsUsed.MSGFSpecEValueThreshold = filterThresholds.MSGFSpecEValueThreshold;
                            }
                        }

                        if (pepFDRValuesArePresent && filterThresholds.UsePepFDRThreshold)
                        {
                            // Typically only MSGFDB results will have PepFDR values
                            if (pepFDR > filterThresholds.PepFDRThreshold)
                            {
                                validPSM = false;
                            }
                            thresholdChecked = true;

                            if (!mFilterThresholdsUsed.UsePepFDRThreshold)
                            {
                                mFilterThresholdsUsed.UsePepFDRThreshold = true;
                                mFilterThresholdsUsed.PepFDRThreshold = filterThresholds.PepFDRThreshold;
                            }
                        }

                        if (fdrValuesArePresent && filterThresholds.UseFDRThreshold)
                        {
                            // Typically only MSGFDB results will have FDR values
                            if (fdr > filterThresholds.FDRThreshold)
                            {
                                validPSM = false;
                            }
                            thresholdChecked = true;

                            if (!mFilterThresholdsUsed.UseFDRThreshold)
                            {
                                mFilterThresholdsUsed.UseFDRThreshold = true;
                                mFilterThresholdsUsed.FDRThreshold = filterThresholds.FDRThreshold;
                            }
                        }

                        if (validPSM && !thresholdChecked)
                        {
                            // Switch to filtering on MSGFSpecEValueThreshold instead of on FDR or PepFDR
                            if (msgfSpecEValue < MSGF_SPEC_EVALUE_NOT_DEFINED && filterThresholds.MSGFSpecEValueThreshold < 0.0001)
                            {
                                if (msgfSpecEValue > filterThresholds.MSGFSpecEValueThreshold)
                                {
                                    validPSM = false;
                                }

                                if (!mFilterThresholdsUsed.UseMSGFSpecEValue)
                                {
                                    mFilterThresholdsUsed.UseMSGFSpecEValue = true;
                                    mFilterThresholdsUsed.MSGFSpecEValueThreshold = filterThresholds.MSGFSpecEValueThreshold;
                                }
                            }
                        }

                        if (validPSM)
                        {
                            // Filter on P-value
                            if (pValue >= filterThresholds.PValueThreshold)
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
                                // If the search engine is MSGFDB and the protein name starts with REV_ or XXX_, skip this protein since it's a decoy result
                                // Otherwise, add the protein to mCachedProteins and mCachedProteinPSMCounts, though we won't know its sequence

                                var proteinUCase = reader.CurrentPSM.ProteinFirst.ToUpper();

                                if (dataPkgJob.PeptideHitResultType == PeptideHitResultTypes.MSGFPlus)
                                {
                                    if (proteinUCase.StartsWith("REV_") || proteinUCase.StartsWith("XXX_"))
                                    {
                                        validPSM = false;
                                    }
                                }
                                else
                                {
                                    if (proteinUCase.StartsWith("REVERSED_") || proteinUCase.StartsWith("SCRAMBLED_") ||
                                        proteinUCase.StartsWith("XXX_") || proteinUCase.StartsWith("XXX."))
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
                            case PeptideHitResultTypes.Sequest:
                                totalPRMScore = reader.CurrentPSM.GetScore(SequestSynFileReader.GetColumnNameByID(SequestSynopsisFileColumns.Sp));
                                pValueFormatted = pValue.ToString("0.00");
                                deltaScore = reader.CurrentPSM.GetScore(SequestSynFileReader.GetColumnNameByID(SequestSynopsisFileColumns.DeltaCn));
                                deltaScoreOther = reader.CurrentPSM.GetScore(SequestSynFileReader.GetColumnNameByID(SequestSynopsisFileColumns.DeltaCn2));
                                break;

                            case PeptideHitResultTypes.XTandem:
                                totalPRMScore = reader.CurrentPSM.GetScore(XTandemSynFileReader.GetColumnNameByID(XTandemSynFileColumns.Hyperscore));
                                pValueFormatted = pValue.ToString("0.00");
                                deltaScore = reader.CurrentPSM.GetScore(XTandemSynFileReader.GetColumnNameByID(XTandemSynFileColumns.DeltaCn2));
                                break;

                            case PeptideHitResultTypes.Inspect:
                                totalPRMScore = reader.CurrentPSM.GetScore(InspectSynFileReader.GetColumnNameByID(InspectSynFileColumns.TotalPRMScore));
                                pValueFormatted = reader.CurrentPSM.GetScore(InspectSynFileReader.GetColumnNameByID(InspectSynFileColumns.PValue));
                                deltaScore = reader.CurrentPSM.GetScore(InspectSynFileReader.GetColumnNameByID(InspectSynFileColumns.DeltaScore));
                                deltaScoreOther = reader.CurrentPSM.GetScore(InspectSynFileReader.GetColumnNameByID(InspectSynFileColumns.DeltaScoreOther));
                                break;

                            case PeptideHitResultTypes.MSGFPlus:
                                totalPRMScore = reader.CurrentPSM.GetScore(MSGFPlusSynFileReader.GetColumnNameByID(MSGFPlusSynFileColumns.DeNovoScore));
                                pValueFormatted = reader.CurrentPSM.GetScore(MSGFPlusSynFileReader.GetColumnNameByID(MSGFPlusSynFileColumns.EValue));
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
                            totalPRMScore + "\t0\t0\t0\t0\t" +
                            reader.CurrentPSM.NumTrypticTermini + "\t" +
                            pValueFormatted + "\t0\t" +
                            deltaScore + "\t" +
                            deltaScoreOther + "\t" +
                            reader.CurrentPSM.ResultID + "\t0\t0\t" +
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

                        if (!PeptideCleavageStateCalculator.SplitPrefixAndSuffixFromSequence(
                            reader.CurrentPSM.Peptide, out _, out var prefix, out var suffix))
                        {
                            prefix = string.Empty;
                            suffix = string.Empty;
                        }

                        var newMSGFData = new PseudoMSGFData
                        {
                            ResultID = reader.CurrentPSM.ResultID,
                            Peptide = reader.CurrentPSM.Peptide,
                            CleanSequence = reader.CurrentPSM.PeptideCleanSequence,
                            PrefixResidue = prefix,
                            SuffixResidue = suffix,
                            ScanNumber = reader.CurrentPSM.ScanNumber,
                            ChargeState = reader.CurrentPSM.Charge,
                            PValue = pValueFormatted,
                            MQScore = reader.CurrentPSM.MSGFSpecEValue,
                            TotalPRMScore = totalPRMScore,
                            NTT = reader.CurrentPSM.NumTrypticTermini,
                            MSGFSpecEValue = reader.CurrentPSM.MSGFSpecEValue,
                            DeltaScore = deltaScore,
                            DeltaScoreOther = deltaScoreOther,
                            Protein = reader.CurrentPSM.ProteinFirst
                        };

                        if (newScanNumber)
                        {
                            bestMatchByScanScoreValues.Add(reader.CurrentPSM.ScanNumber, newMSGFData);
                        }
                        else
                        {
                            bestMatchByScanScoreValues[reader.CurrentPSM.ScanNumber] = newMSGFData;
                        }
                    }
                }

                if (JobFileRenameRequired(job))
                {
                    pseudoMsgfFilePath = Path.Combine(mWorkDir, dataPkgJob.Dataset + "_Job" + dataPkgJob.Job + FILE_EXTENSION_PSEUDO_MSGF);
                }
                else
                {
                    pseudoMsgfFilePath = Path.Combine(mWorkDir, dataPkgJob.Dataset + FILE_EXTENSION_PSEUDO_MSGF);
                }

                using (var writer = new StreamWriter(new FileStream(pseudoMsgfFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    // Write the header line
                    writer.WriteLine("#SpectrumFile\t" + "Scan#\t" + "Annotation\t" + "Protein\t" + "Charge\t" + "MQScore\t" + "Length\t" +
                                         "TotalPRMScore\t" + "MedianPRMScore\t" + "FractionY\t" + "FractionB\t" + "Intensity\t" + "NTT\t" +
                                         "p-value\t" + "F-Score\t" + "DeltaScore\t" + "DeltaScoreOther\t" + "RecordNumber\t" + "DBFilePos\t" +
                                         "SpecFilePos\t" + "SpecProb");

                    // Write out the filter-passing matches to the pseudo MSGF text file
                    foreach (var item in bestMatchByScan)
                    {
                        writer.WriteLine(item.Value.Value);
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
                        matchesForProtein = new List<PseudoMSGFData> {
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
        /// <param name="filterThresholds"></param>
        /// <param name="prideReportXMLFilePath">Output: the full path of the newly created .msgf-report.xml file</param>
        /// <returns>True if success, false if an error</returns>
        [Obsolete("No longer used")]
        private bool CreateMSGFReportFile(int job, string dataset, FilterThresholds filterThresholds,
            out string prideReportXMLFilePath)
        {
            var localOrgDBFolder = mMgrParams.GetParam("OrgDbDir");

            var pseudoMSGFData = new Dictionary<string, List<PseudoMSGFData>>();

            prideReportXMLFilePath = string.Empty;

            try
            {
                var templateFileName = AnalysisResourcesPRIDEConverter.GetMSGFReportTemplateFilename(mJobParams, warnIfJobParamMissing: false);

                var orgDBNameGenerated = mJobParams.GetJobParameter(AnalysisJob.PEPTIDE_SEARCH_SECTION,
                                                                     AnalysisResourcesPRIDEConverter.GetGeneratedFastaParamNameForJob(job), string.Empty);

                if (string.IsNullOrEmpty(orgDBNameGenerated))
                {
                    LogError("Job parameter " + AnalysisResourcesPRIDEConverter.GetGeneratedFastaParamNameForJob(job) +
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
                    // Need to read the proteins from the FASTA file

                    mCachedProteins.Clear();
                    mCachedProteinPSMCounts.Clear();

                    var fastaFilePath = Path.Combine(localOrgDBFolder, orgDBNameGenerated);
                    var fastaFileReader = new ProteinFileReader.FastaFileReader();

                    if (!fastaFileReader.OpenFile(fastaFilePath))
                    {
                        var msg = string.Format("Error opening FASTA file {0}; fastaFileReader.OpenFile() returned false", orgDBNameGenerated);
                        LogError("{0}; see {1}", msg, localOrgDBFolder);
                        mMessage = msg;
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

                    mCachedOrgDBName = orgDBNameGenerated;
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

                var pseudoMsgfFilePath = CreatePseudoMSGFFileUsingPHRPReader(job, dataset, filterThresholds, pseudoMSGFData);

                if (string.IsNullOrEmpty(pseudoMsgfFilePath))
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        LogError("Pseudo Msgf file not created for job " + job + ", dataset " + dataset);
                    }
                    return false;
                }

                AddToListIfNew(mPreviousDatasetFilesToDelete, pseudoMsgfFilePath);

                // Deprecated:
                //if (!mCreateMSGFReportFilesOnly)
                //{
                //    prideReportXMLFilePath = CreateMSGFReportXMLFile(templateFileName, dataPkgJob, pseudoMsgfFilePath, pseudoMSGFData,
                //        orgDBNameGenerated, proteinCollectionListOrFasta, filterThresholds);

                //    if (string.IsNullOrEmpty(prideReportXMLFilePath))
                //    {
                //        if (string.IsNullOrEmpty(mMessage))
                //        {
                //            LogError("Pride report XML file not created for job " + job + ", dataset " + dataset);
                //        }
                //        return false;
                //    }
                //}

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception in CreateMSGFReportFile for job " + job + ", dataset " + dataset, ex);
                return false;
            }
        }

        [Obsolete("No longer used")]
        private string CreateMSGFReportXMLFile(string templateFileName, DataPackageJobInfo dataPkgJob, string pseudoMsgfFilePath,
            IReadOnlyDictionary<string, List<PseudoMSGFData>> pseudoMSGFData, string orgDBNameGenerated,
            string proteinCollectionListOrFasta, FilterThresholds filterThresholds)
        {
            string prideReportXMLFilePath;

            var insideMzDataDescription = false;
            var instrumentDetailsAutoDefined = false;

            var attributeOverride = new Dictionary<string, string>();

            var fileLocation = MSGFReportXMLFileLocations.Header;
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

                using var xmlReader = new XmlTextReader(
                    new FileStream(Path.Combine(mWorkDir, templateFileName), FileMode.Open, FileAccess.Read, FileShare.Read));

                using var writer = new XmlTextWriter(
                    new FileStream(prideReportXMLFilePath, FileMode.Create, FileAccess.Write, FileShare.Read), new UTF8Encoding(false))
                {
                    Formatting = Formatting.Indented,
                    Indentation = 4
                };

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

                            fileLocation = UpdateMSGFReportXMLFileLocation(fileLocation, xmlReader.Name, insideMzDataDescription);

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
                                    if (fileLocation == MSGFReportXMLFileLocations.MzDataAdmin)
                                    {
                                        // Write out the current job's Experiment Name
                                        writer.WriteElementString("sampleName", dataPkgJob.Experiment);
                                        skipNode = true;
                                    }
                                    break;

                                case "sampleDescription":
                                    if (fileLocation == MSGFReportXMLFileLocations.MzDataAdmin)
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
                                    if (fileLocation == MSGFReportXMLFileLocations.MzDataAdmin)
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
                                    if (fileLocation == MSGFReportXMLFileLocations.MzDataDataProcessing)
                                    {
                                        CreateMSGFReportXmlFileWriteSoftwareVersion(xmlReader, writer, dataPkgJob.PeptideHitResultType);
                                        skipNode = true;
                                    }
                                    break;

                                case "instrumentName":
                                    if (fileLocation == MSGFReportXMLFileLocations.MzDataInstrument)
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
                                    if (fileLocation == MSGFReportXMLFileLocations.MzDataInstrument && instrumentDetailsAutoDefined)
                                    {
                                        skipNode = true;
                                    }
                                    break;

                                case "cvParam":
                                    if (fileLocation == MSGFReportXMLFileLocations.ExperimentAdditional)
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
                                    WriteConfigurationOption(writer, "peptide_threshold", filterThresholds.PValueThreshold.ToString("0.00"));
                                    WriteConfigurationOption(writer, "add_carbamidomethylation", "false");

                                    writer.WriteEndElement();      // ConfigurationOptions

                                    skipNode = true;
                                    break;
                            }

                            if (skipNode)
                            {
                                if (xmlReader.NodeType != XmlNodeType.EndElement)
                                {
                                    // Skip this element (and any children nodes enclosed in this element)
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
            catch (Exception ex)
            {
                LogError("Exception in CreateMSGFReportXMLFile", ex);

                var recentElementNames = string.Empty;

                foreach (var item in recentElements)
                {
                    if (string.IsNullOrEmpty(recentElementNames))
                    {
                        recentElementNames = item;
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

        [Obsolete("No longer used")]
        private bool CreateMSGFReportXMLFileWriteIDs(XmlWriter writer,
            IReadOnlyDictionary<string, List<PseudoMSGFData>> pseudoMSGFData, string orgDBNameGenerated)
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
                    foreach (var peptide in proteinEntry.Value)
                    {
                        writer.WriteStartElement("Peptide");

                        writer.WriteElementString("Sequence", peptide.CleanSequence);
                        writer.WriteElementString("CuratedSequence", string.Empty);
                        writer.WriteElementString("Start", "0");
                        writer.WriteElementString("End", "0");
                        writer.WriteElementString("SpectrumReference", peptide.ScanNumber.ToString());

                        // Could write out details of dynamic mods
                        //    Would need to update DMS to include the PSI-Compatible mod names, descriptions, and masses.
                        //    However, since we're now submitting .mzid.gz files to PRIDE and not .msgf-report.xml files, this update is not necessary

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
                        writer.WriteElementString("UniqueIdentifier", peptide.ScanNumber.ToString());

                        writer.WriteStartElement("additional");

                        WriteCVParam(writer, "PRIDE", "PRIDE:0000065", "Upstream flanking sequence", peptide.PrefixResidue);
                        WriteCVParam(writer, "PRIDE", "PRIDE:0000066", "Downstream flanking sequence", peptide.SuffixResidue);

                        WriteCVParam(writer, "MS", "MS:1000041", "charge state", peptide.ChargeState.ToString());
                        WriteCVParam(writer, "MS", "MS:1000042", "peak intensity", "0.0");
                        WriteCVParam(writer, "MS", "MS:1001870", "p-value for peptides", peptide.PValue);

                        WriteUserParam(writer, "MQScore", peptide.MQScore);
                        WriteUserParam(writer, "TotalPRMScore", peptide.TotalPRMScore);

                        // WriteUserParam(writer, "MedianPRMScore", "0.0")
                        // WriteUserParam(writer, "FractionY", "0.0")
                        // WriteUserParam(writer, "FractionB", "0.0")

                        WriteUserParam(writer, "NTT", peptide.NTT.ToString());

                        // WriteUserParam(writer, "F-Score", "0.0")

                        WriteUserParam(writer, "DeltaScore", peptide.DeltaScore);
                        WriteUserParam(writer, "DeltaScoreOther", peptide.DeltaScoreOther);
                        WriteUserParam(writer, "SpecProb", peptide.MSGFSpecEValue);

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

        [Obsolete("No longer used")]
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
                    var proteinName = entry.Key;
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

                writer.WriteEndElement();          // FASTA

                // In the future, we might write out customized PTMs here
                // For now, just copy over whatever is in the template msgf-report.xml file

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

        [Obsolete("No longer used")]
        private void CreateMSGFReportXmlFileWriteSoftwareVersion(XmlReader xmlReader, XmlWriter writer,
            PeptideHitResultTypes PeptideHitResultType)
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
                if (PeptideHitResultType == PeptideHitResultTypes.MSGFPlus && toolName.StartsWith("MSGF", StringComparison.OrdinalIgnoreCase))
                {
                    // Tool Version in the template file is likely correct; use it
                }
                else if (PeptideHitResultType == PeptideHitResultTypes.Sequest && toolName.StartsWith("SEQUEST", StringComparison.OrdinalIgnoreCase))
                {
                    // Tool Version in the template file is likely correct; use it
                }
                else if (PeptideHitResultType == PeptideHitResultTypes.XTandem && toolName.IndexOf("TANDEM", StringComparison.OrdinalIgnoreCase) >= 0)
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
        /// <param name="prideXmlFilePath">Output: the full path of the newly created .msgf-pride.xml file</param>
        /// <returns>True if success, false if an error</returns>
        [Obsolete("No longer used")]
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
                var msgfResultsFilePath = Path.Combine(mWorkDir, baseFileName + FILE_EXTENSION_PSEUDO_MSGF);
                var mzXMLFilePath = Path.Combine(mWorkDir, dataset + AnalysisResources.DOT_MZXML_EXTENSION);
                prideReportXMLFilePath = Path.Combine(mWorkDir, baseFileName + FILE_EXTENSION_MSGF_REPORT_XML);

                var currentTask = "Running PRIDE Converter for job " + job + ", " + dataset;

                if (mDebugLevel >= 1)
                {
                    LogMessage(currentTask);
                }

                var success = RunPrideConverter(job, dataset, msgfResultsFilePath, mzXMLFilePath, prideReportXMLFilePath);

                if (!success)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        LogError("Unknown error calling RunPrideConverter", mMessage);
                    }
                }
                else
                {
                    // Make sure the result file was created
                    prideXmlFilePath = Path.Combine(mWorkDir, baseFileName + FILE_EXTENSION_MSGF_PRIDE_XML);

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

            // Deprecated:
            // var filterText = string.Empty;

            try
            {
                var pXFilePath = Path.Combine(mWorkDir, "PX_Submission_" + DateTime.Now.ToString("yyyy-MM-dd_HH-mm") + ".px");

                var prideXmlFilesCreated = CountResultFilesByType(PXFileInfoBase.PXFileTypes.Result);
                var rawFilesStored = CountResultFilesByType(PXFileInfoBase.PXFileTypes.Raw);
                var peakFilesStored = CountResultFilesByType(PXFileInfoBase.PXFileTypes.Peak);
                var mzIDFilesStored = CountResultFilesByType(PXFileInfoBase.PXFileTypes.ResultMzId);

                if (mDebugLevel >= 1)
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

                    // Deprecated:
                    // if (mFilterThresholdsUsed.UseFDRThreshold || mFilterThresholdsUsed.UsePepFDRThreshold || mFilterThresholdsUsed.UseMSGFSpecEValue)
                    // {
                    //     const string filterTextBase = "msgf-pride.xml files are filtered on ";
                    //     filterText = string.Empty;

                    //     if (mFilterThresholdsUsed.UseFDRThreshold)
                    //     {
                    //         if (string.IsNullOrEmpty(filterText))
                    //         {
                    //             filterText = filterTextBase;
                    //         }
                    //         else
                    //         {
                    //             filterText += " and ";
                    //         }

                    //         filterText += (mFilterThresholdsUsed.FDRThreshold * 100).ToString("0.0") + "% FDR at the PSM level";
                    //     }

                    //     if (mFilterThresholdsUsed.UsePepFDRThreshold)
                    //     {
                    //         if (string.IsNullOrEmpty(filterText))
                    //         {
                    //             filterText = filterTextBase;
                    //         }
                    //         else
                    //         {
                    //             filterText += " and ";
                    //         }

                    //         filterText += (mFilterThresholdsUsed.PepFDRThreshold * 100).ToString("0.0") + "% FDR at the peptide level";
                    //     }

                    //     if (mFilterThresholdsUsed.UseMSGFSpecEValue)
                    //     {
                    //         if (string.IsNullOrEmpty(filterText))
                    //         {
                    //             filterText = filterTextBase;
                    //         }
                    //         else
                    //         {
                    //             filterText += " and ";
                    //         }

                    //         filterText += "MSGF Spectral Probability <= " + mFilterThresholdsUsed.MSGFSpecEValueThreshold.ToString("0.0E+00");
                    //     }
                    // }
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

                using var writer = new StreamWriter(new FileStream(pXFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                WritePXHeader(writer, "submitter_name", "Matthew Monroe", templateParameters, paramsWithCVs);
                WritePXHeader(writer, "submitter_email", "matthew.monroe@pnnl.gov", templateParameters, paramsWithCVs);
                WritePXHeader(writer, "submitter_affiliation", PNNL_NAME_COUNTRY, templateParameters, paramsWithCVs);
                WritePXHeader(writer, "submitter_pride_login", "matthew.monroe@pnnl.gov", templateParameters, paramsWithCVs);

                WritePXHeader(writer, "lab_head_name", "Richard D. Smith", templateParameters, paramsWithCVs);
                WritePXHeader(writer, "lab_head_email", "dick.smith@pnnl.gov", templateParameters, paramsWithCVs);
                WritePXHeader(writer, "lab_head_affiliation", PNNL_NAME_COUNTRY, templateParameters, paramsWithCVs);

                WritePXHeader(writer, "project_title", TBD + "User-friendly Article Title", templateParameters, paramsWithCVs);

                // Minimum 50 characters, max 5000 characters
                WritePXHeader(writer, "project_description", TBD + "Summary sentence", templateParameters, paramsWithCVs, 50);

                // We don't normally use the project_tag field, so it is commented out
                // Example official tags are:
                //  Human proteome project
                //  Human plasma project
                // WritePXHeader(writer, "project_tag", TBD + "Official project tag assigned by the repository", templateParameters)

                if (templateParameters.ContainsKey("pubmed_id"))
                {
                    WritePXHeader(writer, "pubmed_id", TBD, templateParameters, paramsWithCVs);
                }

                // We don't normally use this field, so it is commented out
                // WritePXHeader(writer, "other_omics_link", "Related data is available from PeptideAtlas at http://www.peptideatlas.org/PASS/PASS00297")

                // Comma separated list; suggest at least 3 keywords
                WritePXHeader(writer, "keywords", TBD, templateParameters, paramsWithCVs);

                // Minimum 50 characters, max 5000 characters
                WritePXHeader(writer, "sample_processing_protocol", TBD, templateParameters, paramsWithCVs, 50);

                // Minimum 50 characters, max 5000 characters
                WritePXHeader(writer, "data_processing_protocol", TBD, templateParameters, paramsWithCVs, 50);

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
                WritePXHeader(writer, "experiment_type", defaultExperiment, templateParameters, paramsWithCVs);

                WritePXLine(writer, new List<string>
                {
                    "MTD",
                    "submission_type",
                    submissionType
                });

                if (submissionType == COMPLETE_SUBMISSION)
                {
                    // Deprecated:
                    // // Note that the comment field has been deprecated in v2.x of the px file
                    // // However, we don't have a good alternative place to put this comment, so we'll include it anyway
                    // if (!string.IsNullOrWhiteSpace(filterText))
                    // {
                    //     WritePXHeader(writer, "comment", filterText, paramsWithCVs);
                    // }
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

                    WritePXHeader(writer, "reason_for_partial", comment, paramsWithCVs);
                }

                var mouseOrHuman = false;

                if (mExperimentNEWTInfo.Count == 0)
                {
                    // None of the data package jobs had valid NEWT info
                    var defaultSpecies = TBD + GetCVString("NEWT", "2323", "unclassified Bacteria");
                    WritePXHeader(writer, "species", defaultSpecies, templateParameters, paramsWithCVs);
                }
                else
                {
                    // NEWT info is defined; write it out
                    foreach (var item in mExperimentNEWTInfo)
                    {
                        WritePXHeader(writer, "species", GetNEWTCv(item.Key, item.Value), paramsWithCVs);

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
                        WritePXHeader(writer, "tissue", GetCVString("BTO", item.Key, item.Value), templateParameters, paramsWithCVs);
                    }
                }
                else
                {
                    string defaultTissue;

                    if (mouseOrHuman)
                        defaultTissue = TBD + DEFAULT_TISSUE_CV_MOUSE_HUMAN;
                    else
                        defaultTissue = TBD + DEFAULT_TISSUE_CV;

                    WritePXHeader(writer, "tissue", defaultTissue, templateParameters, paramsWithCVs);
                }

                const string defaultCellType = TBD + "Optional, e.g. " + DEFAULT_CELL_TYPE_CV + DELETION_WARNING;
                const string defaultDisease = TBD + "Optional, e.g. " + DEFAULT_DISEASE_TYPE_CV + DELETION_WARNING;

                WritePXHeader(writer, "cell_type", defaultCellType, templateParameters, paramsWithCVs);
                WritePXHeader(writer, "disease", defaultDisease, templateParameters, paramsWithCVs);

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
                const string defaultQuantCV = TBD + "Optional, e.g. " + DEFAULT_QUANTIFICATION_TYPE_CV;
                WritePXHeader(writer, "quantification", defaultQuantCV, templateParameters, paramsWithCVs);

                if (mInstrumentGroupsStored.Count > 0)
                {
                    WritePXInstruments(writer, paramsWithCVs);
                }
                else
                {
                    // Instrument type is unknown
                    var defaultInstrument = TBD + GetCVString("MS", "MS:1000031", "instrument model", "CUSTOM UNKNOWN MASS SPEC");
                    WritePXHeader(writer, "instrument", defaultInstrument, templateParameters, paramsWithCVs);
                }

                // Note that the modification terms are optional for complete submissions
                // However, it doesn't hurt to include them
                WritePXMods(writer, paramsWithCVs);

                // Could write additional terms here
                // WritePXHeader(writer, "additional", GetCVString("", "", "Patient", "Cancer patient 1"), templateParameters)

                // If this is a re-submission or re-analysis, use these:
                // WritePXHeader(writer, "resubmission_px", "PXD00001", templateParameters)
                // WritePXHeader(writer, "reanalysis_px", "PXD00001", templateParameters)

                // Add a blank line
                writer.WriteLine();

                // Write the header row for the files
                WritePXLine(writer, new List<string>
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
                    fileInfoCols.Add(Path.Combine(@"D:\Upload", mResultsDirectoryName, item.Value.Filename));

                    var fileMappings = new List<string>();

                    foreach (var mapID in item.Value.FileMappings)
                    {
                        // file_mapping
                        fileMappings.Add(mapID.ToString());
                    }

                    fileInfoCols.Add(string.Join(",", fileMappings));

                    WritePXLine(writer, fileInfoCols);

                    if (fileTypeName == "RESULT")
                    {
                        resultFileIDs.Add(item.Key, item.Value.Filename);
                    }
                }

                // Determine whether the tissue or cell_type columns will be in the SMH section
                var smhIncludesCellType = DictionaryHasDefinedValue(templateParameters, "cell_type");
                var smhIncludesDisease = DictionaryHasDefinedValue(templateParameters, "disease");

                var reJobAddon = new Regex(@"(_Job\d+)(_msgfplus)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                writer.WriteLine();

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

                WritePXLine(writer, columnNames);

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
                            // Mod CVs must be comma separated, with no space after the comma
                            if (mods.Length > 0)
                                mods += ",";

                            mods += GetCVString(modEntry.Value);
                        }

                        // Modification
                        fileInfoCols.Add(mods);

                        GetInstrumentAccession(sampleMetadata.InstrumentGroup, sampleMetadata.InstrumentName, out var instrumentAccession, out var instrumentDescription);

                        var instrumentCV = GetInstrumentCv(instrumentAccession, instrumentDescription);

                        fileInfoCols.Add(instrumentCV);

                        fileInfoCols.Add(GetValueOrDefault("quantification)", templateParameters, sampleMetadata.Quantification));

                        fileInfoCols.Add(sampleMetadata.ExperimentalFactor);
                    }
                    else
                    {
                        LogWarning(" Sample Metadata not found for " + resultFile.Value);
                    }

                    WritePXLine(writer, fileInfoCols);
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
            mJobParams.AddResultFileExtensionToSkip(FILE_EXTENSION_PSEUDO_MSGF);
            mJobParams.AddResultFileExtensionToSkip(FILE_EXTENSION_MSGF_REPORT_XML);
            mJobParams.AddResultFileExtensionToSkip(AnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX);

            mJobParams.AddResultFileToSkip("PRIDEConverter_ConsoleOutput.txt");
            mJobParams.AddResultFileToSkip("PRIDEConverter_Version.txt");

            var workingDirectory = new DirectoryInfo(mWorkDir);

            foreach (var fileToSkip in workingDirectory.GetFiles(DataPackageFileHandler.JOB_INFO_FILE_PREFIX + "*.txt"))
            {
                mJobParams.AddResultFileToSkip(fileToSkip.Name);
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
                if (string.IsNullOrEmpty(mMessage))
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
            if (!mPxResultFiles.TryGetValue(fileID, out var pxFileInfo))
            {
                LogError("FileID " + fileID + " not found in mPxResultFiles; unable to add parent file");
                return false;
            }

            pxFileInfo.AddFileMapping(parentFileID);

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

        private bool FileExistsInTransferDirectory(string remoteTransferDirectory, string filePath, string optionalSuffix = "")
        {
            var fileName = Path.GetFileName(filePath);

            if (fileName == null)
                return false;

            if (File.Exists(Path.Combine(remoteTransferDirectory, fileName)))
                return true;

            if (string.IsNullOrWhiteSpace(optionalSuffix))
            {
                return false;
            }

            return File.Exists(Path.Combine(remoteTransferDirectory, fileName + optionalSuffix));
        }

        private string GetCVString(SampleMetadata.CvParamInfo cvParamInfo)
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
            if (string.IsNullOrEmpty(accession))
            {
                return GetCVString("MS", "MS:1000031", "instrument model", "CUSTOM UNKNOWN MASS SPEC");
            }

            return GetCVString("MS", accession, description);
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
        /// Determines the Accession and Description for the given instrument group
        /// </summary>
        /// <param name="instrumentGroup"></param>
        /// <param name="instrumentName"></param>
        /// <param name="accession">Output parameter</param>
        /// <param name="description">Output parameter</param>
        private void GetInstrumentAccession(string instrumentGroup, string instrumentName, out string accession, out string description)
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
                    accession = "MS:1001545";
                    description = "Bruker Daltonics amaZon series";
                    break;

                case "Bruker_FTMS":
                case "BrukerFT_BAF":
                    accession = "MS:1001548";
                    description = "Bruker Daltonics solarix series";
                    break;

                case "Bruker_QTOF":
                    accession = "MS:1001535";
                    description = "Bruker Daltonics BioTOF series";
                    break;

                case "Exactive":
                    accession = "MS:1000649";
                    description = "Exactive";
                    break;

                case "TSQ":
                case "GC-TSQ":
                    if (instrumentName.Equals("TSQ_1") || instrumentName.Equals("TSQ_2"))
                    {
                        // TSQ_1 and TSQ_2 are TSQ Quantum Ultra instruments
                        accession = "MS:1000751";
                        description = "TSQ Quantum Ultra";
                    }
                    else
                    {
                        // TSQ_3, TSQ_4, and TSQ_5 are TSQ Vantage instruments
                        accession = "MS:1001510";
                        description = "TSQ Vantage";
                    }
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
                case "GC-QExactive":
                case "QEHFX":
                    if (instrumentName.StartsWith("QExactHF", StringComparison.OrdinalIgnoreCase))
                    {
                        accession = "MS:1002523";
                        description = "Q Exactive HF";
                    }
                    else if (instrumentName.Contains("HFX"))
                    {
                        accession = "MS:1002877";
                        description = "Q Exactive HF-X";
                    }
                    else if (instrumentName.StartsWith("QExactP", StringComparison.OrdinalIgnoreCase))
                    {
                        accession = "MS:1002634";
                        description = "Q Exactive Plus";
                    }
                    else
                    {
                        accession = "MS:1001911";
                        description = "Q Exactive";
                    }
                    break;

                case "QTrap":
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
                    accession = "MS:1003096";
                    description = "LTQ Orbitrap Velos Pro";
                    break;

                case "Eclipse":
                    accession = "MS:1003029";
                    description = "Orbitrap Eclipse";
                    break;

                case "Exploris":
                    if (instrumentName.StartsWith("Exploris02", StringComparison.OrdinalIgnoreCase))
                    {
                        accession = "MS:1003094";
                        description = "Orbitrap Exploris 240";
                    }
                    else
                    {
                        // Exploris01 is a 480, but coupled to FT-ICR; Exploris03 is also a 480
                        accession = "MS:1003028";
                        description = "Orbitrap Exploris 480";
                    }
                    break;

                case "Ascend":
                    accession = "MS:1003356";
                    description = "Orbitrap Ascend";
                    break;
            }
        }

        [Obsolete("No longer used")]
        private string GetPrideConverterVersion(string prideConverterProgLoc)
        {
            mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mStatusTools.CurrentOperation = "Determining PrideConverter Version";
            mStatusTools.UpdateAndWrite(mProgress);
            var versionFilePath = Path.Combine(mWorkDir, "PRIDEConverter_Version.txt");

            var arguments = "-jar " + PossiblyQuotePath(prideConverterProgLoc) +
                            " -converter" +
                            " -version";

            if (mDebugLevel >= 2)
            {
                LogDebug(mJavaProgLoc + " " + arguments);
            }

            mCmdRunner.CreateNoWindow = false;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = false;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = versionFilePath;
            mCmdRunner.WorkDir = mWorkDir;

            var success = mCmdRunner.RunProgram(mJavaProgLoc, arguments, "PrideConverter", true);

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
                var versionFile = new FileInfo(versionFilePath);

                if (!versionFile.Exists)
                    return "unknown";

                // Open the version file and read the version
                using var reader = new StreamReader(new FileStream(versionFile.FullName, FileMode.Open, FileAccess.Read));

                if (!reader.EndOfStream)
                {
                    var prideConverterVersion = reader.ReadLine();
                    return prideConverterVersion;
                }
            }

            return "unknown";
        }

        private string GetValueOrDefault(string type, IReadOnlyDictionary<string, string> parameters, string defaultValue)
        {
            if (parameters.TryGetValue(type, out var valueOverride))
            {
                return valueOverride;
            }

            return defaultValue;
        }

        private void InitializeOptions()
        {
            // Update the processing options

            // Deprecated:
            // mCreatePrideXMLFiles = mJobParams.GetJobParameter("CreatePrideXMLFiles", false);
            // mCreateMSGFReportFilesOnly = mJobParams.GetJobParameter("CreateMSGFReportFilesOnly", false);
            // mIncludePepXMLFiles = mJobParams.GetJobParameter("IncludePepXMLFiles", true);

            mCreateMGFFiles = mJobParams.GetJobParameter("CreateMGFFiles", true);

            mProcessMzIdFiles = mJobParams.GetJobParameter("IncludeMzIdFiles", true);

            // Deprecated:
            // if (mCreateMSGFReportFilesOnly)
            // {
            //     mCreateMGFFiles = false;
            //     mIncludePepXMLFiles = false;
            //     mProcessMzIdFiles = false;
            //     mCreatePrideXMLFiles = false;
            // }

            // Initialize the protein dictionaries
            mCachedProteins = new Dictionary<string, KeyValuePair<int, string>>();
            mCachedProteinPSMCounts = new Dictionary<int, int>();

            // Initialize the PXFile lists
            mPxMasterFileList = new Dictionary<string, PXFileInfoBase>(StringComparer.OrdinalIgnoreCase);
            mPxResultFiles = new Dictionary<int, PXFileInfo>();

            // Initialize the CDTAFileStats dictionary
            mCDTAFileStats = new Dictionary<string, PXFileInfoBase>(StringComparer.OrdinalIgnoreCase);

            // Clear the previous dataset objects
            mPreviousDatasetName = string.Empty;
            mPreviousDatasetFilesToDelete = new List<string>();
            mPreviousDatasetFilesToCopy = new List<string>();

            // Initialize additional items

            // Deprecated: mFilterThresholdsUsed = new filterThresholdsType();
            mInstrumentGroupsStored = new Dictionary<string, SortedSet<string>>();
            mSearchToolsUsed = new SortedSet<string>();
            mExperimentNEWTInfo = new Dictionary<int, string>();
            mExperimentTissue = new Dictionary<string, string>();

            mModificationsUsed = new Dictionary<string, SampleMetadata.CvParamInfo>(StringComparer.OrdinalIgnoreCase);

            mMzIdSampleInfo = new Dictionary<string, SampleMetadata>(StringComparer.OrdinalIgnoreCase);

            // Deprecated:
            // // Determine the filter thresholds
            // var filterThresholds = new filterThresholdsType();
            // filterThresholds.Clear();
            // filterThresholds.PValueThreshold = mJobParams.GetJobParameter("PValueThreshold", filterThresholds.PValueThreshold);
            // filterThresholds.FDRThreshold = mJobParams.GetJobParameter("FDRThreshold", filterThresholds.FDRThreshold);
            // filterThresholds.PepFDRThreshold = mJobParams.GetJobParameter("PepFDRThreshold", filterThresholds.PepFDRThreshold);

            // // Support both SpecProb and SpecEValue job parameters
            // filterThresholds.MSGFSpecEValueThreshold = mJobParams.GetJobParameter("MSGFSpecProbThreshold", filterThresholds.MSGFSpecEValueThreshold);
            // filterThresholds.MSGFSpecEValueThreshold = mJobParams.GetJobParameter("MSGFSpecEvalueThreshold", filterThresholds.MSGFSpecEValueThreshold);

            // filterThresholds.UseFDRThreshold = mJobParams.GetJobParameter("UseFDRThreshold", filterThresholds.UseFDRThreshold);
            // filterThresholds.UsePepFDRThreshold = mJobParams.GetJobParameter("UsePepFDRThreshold", filterThresholds.UsePepFDRThreshold);

            // // Support both SpecProb and SpecEValue job parameters
            // filterThresholds.UseMSGFSpecEValue = mJobParams.GetJobParameter("UseMSGFSpecProb", filterThresholds.UseMSGFSpecEValue);
            // filterThresholds.UseMSGFSpecEValue = mJobParams.GetJobParameter("UseMSGFSpecEValue", filterThresholds.UseMSGFSpecEValue);

            //return filterThresholds;
        }

        /// <summary>
        /// Returns true if the there are multiple jobs in mDataPackagePeptideHitJobs for the dataset for the specified job
        /// </summary>
        /// <param name="job"></param>
        /// <returns>True if this job's dataset has multiple jobs in mDataPackagePeptideHitJobs, otherwise false</returns>
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
                mDataPackagePeptideHitJobs = new Dictionary<int, DataPackageJobInfo>();
            }
            else
            {
                mDataPackagePeptideHitJobs.Clear();
            }

            var dataPackagePeptideHitJobs = RetrieveDataPackagePeptideHitJobInfo(out var additionalJobs, out var errorMsg);

            if (dataPackagePeptideHitJobs.Count == 0)
            {
                const string msg = "Error loading data package job info";

                if (string.IsNullOrEmpty(errorMsg))
                    LogError(msg + ": RetrieveDataPackagePeptideHitJobInfo returned no jobs");
                else
                    LogError(msg + ": " + errorMsg);

                return false;
            }

            var jobsToUse = ExtractPackedJobParameterList(AnalysisResourcesPRIDEConverter.JOB_PARAM_DATA_PACKAGE_PEPTIDE_HIT_JOBS);

            if (jobsToUse.Count == 0)
            {
                LogWarning("Packed job parameter " + AnalysisResourcesPRIDEConverter.JOB_PARAM_DATA_PACKAGE_PEPTIDE_HIT_JOBS +
                           " is empty; no jobs to process");
            }
            else
            {
                var dataPackageJobs = new Dictionary<int, DataPackageJobInfo>();

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
        [Obsolete("No longer used")]
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            // ReSharper disable CommentTypo

            // Example Console output:

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

            // ReSharper restore CommentTypo

            try
            {
                if (!File.Exists(consoleOutputFilePath))
                {
                    if (mDebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + consoleOutputFilePath);
                    }

                    return;
                }

                if (mDebugLevel >= 4)
                {
                    LogDebug("Parsing file " + consoleOutputFilePath);
                }

                mConsoleOutputErrorMsg = string.Empty;

                using var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (!string.IsNullOrWhiteSpace(dataLine))
                    {
                        if (dataLine.IndexOf(" error ", StringComparison.OrdinalIgnoreCase) >= 0)
                        {
                            if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                            {
                                mConsoleOutputErrorMsg = "Error running Pride Converter:";
                            }
                            mConsoleOutputErrorMsg += "; " + dataLine;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        /// <summary>
        /// Process one job
        /// </summary>
        /// <param name="jobInfo">Keys are job numbers and values contain job info</param>
        /// <param name="analysisResults"></param>
        /// <param name="dataPackageDatasets"></param>
        /// <param name="remoteTransferDirectory"></param>
        /// <param name="datasetRawFilePaths"></param>t
        /// <param name="templateParameters"></param>
        /// <param name="assumeInstrumentDataUnpurged"></param>
        private CloseOutType ProcessJob(
            KeyValuePair<int, DataPackageJobInfo> jobInfo,
            AnalysisResults analysisResults,
            IReadOnlyDictionary<int, DataPackageDatasetInfo> dataPackageDatasets,
            string remoteTransferDirectory,
            IReadOnlyDictionary<string, string> datasetRawFilePaths,
            IReadOnlyDictionary<string, string> templateParameters,
            bool assumeInstrumentDataUnpurged)
        {
            var resultFiles = new ResultFileContainer();

            var job = jobInfo.Value.Job;
            var dataset = jobInfo.Value.Dataset;

            if (mPreviousDatasetName != dataset)
            {
                TransferPreviousDatasetFiles(analysisResults, remoteTransferDirectory);

                // Retrieve the dataset files for this dataset
                mPreviousDatasetName = dataset;

                // Deprecated:
                // if (mCreatePrideXMLFiles && !mCreateMSGFReportFilesOnly)
                // {
                //     // Create the .mzXML files if it is missing
                //     success = CreateMzXMLFileIfMissing(dataset, analysisResults, datasetRawFilePaths);
                //     if (!success)
                //     {
                //         return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                //     }
                // }
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

            // Retrieve the PHRP files, MS-GF+ results, and _dta.txt or .mzML.gz file for this job
            var filesCopied = new List<string>();

            var success = RetrievePHRPFiles(job, dataset, analysisResults, remoteTransferDirectory, filesCopied);

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
                if (FileExistsInTransferDirectory(remoteTransferDirectory, dataset + DOT_MGF))
                {
                    // The .mgf file already exists on the remote server; update .MGFFilePath
                    // The path to the file doesn't matter; just the name
                    resultFiles.MGFFilePath = Path.Combine(mWorkDir, dataset + DOT_MGF);
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
                    resultFiles.MGFFilePath = Path.Combine(mWorkDir, dataset + DOT_MZML_GZ);
                }
                else
                {
                    resultFiles.MGFFilePath = Path.Combine(mWorkDir, dataset + AnalysisResources.CDTA_EXTENSION);
                }

                if (!assumeInstrumentDataUnpurged && !searchedMzML && !File.Exists(resultFiles.MGFFilePath))
                {
                    // .mgf file not found
                    // We don't check for .mzML.gz files since those are not copied locally if they already exist in remoteTransferDirectory
                    resultFiles.MGFFilePath = string.Empty;
                }
            }

            // Update the .mzid.gz file(s) for this job

            if (mProcessMzIdFiles && jobInfo.Value.PeptideHitResultType == PeptideHitResultTypes.MSGFPlus)
            {
                mMessage = string.Empty;

                success = UpdateMzIdFiles(remoteTransferDirectory, jobInfo.Value, datasetInfo, searchedMzML, out var mzIdFilePaths, out _, templateParameters);

                if (!success || mzIdFilePaths == null || mzIdFilePaths.Count == 0)
                {
                    if (string.IsNullOrEmpty(mMessage))
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

            // Deprecated
            // if (mIncludePepXMLFiles && jobInfo.Value.PeptideHitResultType != PeptideHitResultTypes.Unknown ||
            //     jobInfo.Value.PeptideHitResultType == PeptideHitResultTypes.Sequest)
            // {
            //     var pepXmlFilename = jobInfo.Value.Dataset + ".pepXML";
            //     var pepXMLFile = new FileInfo(Path.Combine(mWorkDir, pepXmlFilename));
            //     if (pepXMLFile.Exists)
            //     {
            //         // Make sure it is capitalized correctly, then gzip it

            //         if (!string.Equals(pepXMLFile.Name, pepXmlFilename, StringComparison.Ordinal))
            //         {
            //             pepXMLFile.MoveTo(pepXMLFile.FullName + ".tmp");
            //             pepXMLFile.MoveTo(Path.Combine(mWorkDir, pepXmlFilename));
            //         }

            //         // Note that the original file will be auto-deleted after the .gz file is created
            //         var gzippedPepXMLFile = GZipFile(pepXMLFile);

            //         if (gzippedPepXMLFile == null)
            //         {
            //             if (string.IsNullOrEmpty(mMessage))
            //             {
            //                 LogError("GZipFile returned false for " + pepXMLFile.FullName);
            //             }
            //             return CloseOutType.CLOSEOUT_FAILED;
            //         }

            //         resultFiles.PepXMLFile = gzippedPepXMLFile.FullName;
            //     }
            // }

            // Store the instrument group and instrument name
            StoreInstrumentInfo(jobInfo.Value);

            resultFiles.PrideXmlFilePath = string.Empty;

            // Deprecated:
            //if (mCreatePrideXMLFiles)
            //{
            //    // Create the .msgf-report.xml file for this job

            //    success = CreateMSGFReportFile(job, dataset, filterThresholds, out var prideReportXMLFilePath);
            //    if (!success)
            //    {
            //        return CloseOutType.CLOSEOUT_FAILED;
            //    }

            //    AddToListIfNew(mPreviousDatasetFilesToDelete, prideReportXMLFilePath);

            //    if (!mCreateMSGFReportFilesOnly)
            //    {
            //        // Create the .msgf-Pride.xml file for this job
            //        success = CreatePrideXMLFile(job, dataset, prideReportXMLFilePath, out var prideXmlPath);
            //        resultFiles.PrideXmlFilePath = prideXmlPath;
            //        if (!success)
            //        {
            //            return CloseOutType.CLOSEOUT_FAILED;
            //        }
            //    }
            //}

            success = AppendToPXFileInfo(jobInfo.Value, datasetRawFilePaths, resultFiles);

            if (success)
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            return CloseOutType.CLOSEOUT_FAILED;
        }

        private string PXFileTypeName(PXFileInfoBase.PXFileTypes pxFileType)
        {
            return pxFileType switch
            {
                PXFileInfoBase.PXFileTypes.Result => "RESULT",
                PXFileInfoBase.PXFileTypes.ResultMzId => "RESULT",
                PXFileInfoBase.PXFileTypes.Raw => "RAW",
                PXFileInfoBase.PXFileTypes.Search => "SEARCH",
                PXFileInfoBase.PXFileTypes.Peak => "PEAK",
                _ => "OTHER"
            };
        }

        /// <summary>
        /// Reads the template PX Submission file
        /// Caches the keys and values for the method lines (which start with MTD)
        /// </summary>
        /// <returns>Dictionary of keys and values</returns>
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
                var templateFileName = AnalysisResourcesPRIDEConverter.GetPXSubmissionTemplateFilename(mJobParams, WarnIfJobParamMissing: false);
                var templateFilePath = Path.Combine(mWorkDir, templateFileName);

                if (!File.Exists(templateFilePath))
                {
                    return parameters;
                }

                using var reader = new StreamReader(new FileStream(templateFilePath, FileMode.Open, FileAccess.Read, FileShare.Read));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrEmpty(dataLine))
                        continue;

                    if (!dataLine.StartsWith("MTD"))
                        continue;

                    var dataCols = dataLine.Split(new[] { '\t' }, 3).ToList();

                    if (dataCols.Count < 3 || string.IsNullOrEmpty(dataCols[1]))
                        continue;

                    var keyName = dataCols[1];

                    // Automatically rename parameters updated from v1.x to v2.x of the .px file format
                    if (keyNameOverrides.TryGetValue(keyName, out var keyNameNew))
                    {
                        keyName = keyNameNew;
                    }

                    if (!string.Equals(keyName, OBSOLETE_FIELD_FLAG) && !parameters.ContainsKey(keyName))
                    {
                        parameters.Add(keyName, dataCols[2].Trim());
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

        private SampleMetadata.CvParamInfo ReadWriteCvParam(XmlReader xmlReader, XmlWriter writer,
            Stack<int> elementCloseDepths)
        {
            var cvParam = new SampleMetadata.CvParamInfo();
            cvParam.Clear();

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
                            cvParam.Accession = xmlReader.Value;
                            break;
                        case "cvRef":
                            cvParam.CvRef = xmlReader.Value;
                            break;
                        case "name":
                            cvParam.Name = xmlReader.Value;
                            break;
                        case "value":
                            cvParam.Value = xmlReader.Value;
                            break;
                        case "unitCvRef":
                            cvParam.unitCvRef = xmlReader.Value;
                            break;
                        case "unitName":
                            cvParam.unitName = xmlReader.Value;
                            break;
                        case "unitAccession":
                            cvParam.unitAccession = xmlReader.Value;
                            break;
                    }
                } while (xmlReader.MoveToNextAttribute());

                elementCloseDepths.Push(xmlReader.Depth);
            }
            else if (xmlReader.IsEmptyElement)
            {
                writer.WriteEndElement();
            }

            return cvParam;
        }

        private bool RetrievePHRPFiles(
            int job,
            string dataset,
            AnalysisResults analysisResults,
            string remoteTransferDirectory,
            ICollection<string> filesCopied)
        {
            var filesToCopy = new List<string>();

            try
            {
                var jobInfoFilePath = DataPackageFileHandler.GetJobInfoFilePath(job, mWorkDir);

                if (!File.Exists(jobInfoFilePath))
                {
                    // Assume the files already exist
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
                        if (sourceFilePath.EndsWith(AnalysisResources.CDTA_ZIPPED_EXTENSION, StringComparison.OrdinalIgnoreCase))
                            cdtaFile = true;
                        else if (sourceFilePath.EndsWith(DOT_MZID_GZ, StringComparison.OrdinalIgnoreCase))
                            mzidFiles.Add(Path.GetFileName(sourceFilePath));
                        else
                            otherFiles.Add(Path.GetFileName(sourceFilePath));
                    }

                    if (otherFiles.Count == 0 && cdtaFile && mzidFiles.Count > 0)
                    {
                        if (FileExistsInTransferDirectory(remoteTransferDirectory, dataset + DOT_MGF))
                        {
                            var allowSkip = mzidFiles.All(remoteMzIdFile => FileExistsInTransferDirectory(remoteTransferDirectory, remoteMzIdFile));

                            if (allowSkip)
                            {
                                LogDebug("Skipping job {0} since the .mgf and .mzid.gz files already exist at {1}", job, remoteTransferDirectory);

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
                    if (sourceFilePath.StartsWith(AnalysisResources.MYEMSL_PATH_FLAG))
                    {
                        // Make sure the myEMSLUtilities object knows about this dataset
                        mMyEMSLUtilities.AddDataset(dataset);
                        DatasetInfoBase.ExtractMyEMSLFileID(sourceFilePath, out var cleanFilePath);

                        var sourceFileClean = new FileInfo(cleanFilePath);
                        var unzipRequired = string.Equals(sourceFileClean.Extension, ".zip", StringComparison.OrdinalIgnoreCase);

                        mMyEMSLUtilities.AddFileToDownloadQueue(sourceFilePath, unzipRequired);

                        filesCopied.Add(sourceFileClean.Name);

                        continue;
                    }

                    var sourceFile = new FileInfo(sourceFilePath);

                    if (!sourceFile.Exists)
                    {
                        fileCountNotFound++;
                        LogError("File not found for job {0}: {1}", job, sourceFilePath);
                        continue;
                    }

                    var targetFilePath = Path.Combine(mWorkDir, sourceFile.Name);

                    var localFile = new FileInfo(targetFilePath);
                    var alreadyCopiedToTransferDirectory = false;

                    if (sourceFile.Name.EndsWith(DOT_MZML, StringComparison.OrdinalIgnoreCase) ||
                        sourceFile.Name.EndsWith(DOT_MZML_GZ, StringComparison.OrdinalIgnoreCase))
                    {
                        // mzML files can be large
                        // If the file already exists in the transfer directory and the sizes match, do not recopy

                        var fileInTransferDirectory = new FileInfo(Path.Combine(remoteTransferDirectory, sourceFile.Name));

                        if (fileInTransferDirectory.Exists)
                        {
                            if (fileInTransferDirectory.Length == sourceFile.Length)
                            {
                                alreadyCopiedToTransferDirectory = true;
                                LogDebug("Skipping file {0} since already copied to {1}", sourceFile.Name, remoteTransferDirectory);
                            }
                        }
                    }

                    if (alreadyCopiedToTransferDirectory)
                    {
                        filesCopied.Add(sourceFile.Name);
                    }
                    else
                    {
                        if (sourceFile.FullName.Equals(localFile.FullName))
                        {
                            // The file already exists in the working directory
                            // This is likely a data caching logic error, since, even if the file is copied locally (e.g. renaming from _msgfplus.zip to _msgfplus.mzid.gz), the file should have been stored in a subdirectory
                            LogError("File already found in the working directory; this is likely a code logic error: " + localFile.FullName);
                            return false;
                        }

                        // Retrieve the file, allowing for up to 3 attempts (uses CopyFileUsingLocks)
                        analysisResults.CopyFileWithRetry(sourceFile.FullName, localFile.FullName, true);

                        if (!localFile.Exists)
                        {
                            LogError("PHRP file was not copied locally: " + localFile.Name);
                            return false;
                        }

                        filesCopied.Add(sourceFile.Name);

                        var unzipped = false;

                        // Decompress .zip files
                        // Do not decompress .gz files since we can decompress them on-the-fly while reading them
                        if (string.Equals(localFile.Extension, ".zip", StringComparison.OrdinalIgnoreCase))
                        {
                            // Decompress the .zip file
                            mZipTools.UnzipFile(localFile.FullName, mWorkDir);
                            unzipped = true;
                        }

                        if (unzipped)
                        {
                            foreach (var unzippedFile in mZipTools.MostRecentUnzippedFiles)
                            {
                                filesCopied.Add(unzippedFile.Key);
                                AddToListIfNew(mPreviousDatasetFilesToDelete, unzippedFile.Value);
                            }
                        }
                    }

                    AddToListIfNew(mPreviousDatasetFilesToDelete, localFile.FullName);
                }

                if (mMyEMSLUtilities.FilesToDownload.Count > 0)
                {
                    if (!mMyEMSLUtilities.ProcessMyEMSLDownloadQueue(mWorkDir, Downloader.DownloadLayout.FlatNoSubdirectories))
                    {
                        if (string.IsNullOrWhiteSpace(mMessage))
                        {
                            mMessage = "ProcessMyEMSLDownloadQueue return false";
                        }
                        return false;
                    }

                    if (mMyEMSLUtilities.FilesToDownload.Count > 0)
                    {
                        // The queue should have already been cleared; checking just in case
                        mMyEMSLUtilities.ClearDownloadQueue();
                    }
                }

                return fileCountNotFound == 0;
            }
            catch (Exception ex)
            {
                LogError("Error in RetrievePHRPFiles", ex);
                return false;
            }
        }

        private bool RetrieveStoragePathInfoTargetFile(
            string storagePathInfoFilePath,
            AnalysisResults analysisResults,
            out string destinationPath)
        {
            return RetrieveStoragePathInfoTargetFile(storagePathInfoFilePath, analysisResults, IsFolder: false, destinationPath: out destinationPath);
        }

        private bool RetrieveStoragePathInfoTargetFile(
            string storagePathInfoFilePath,
            AnalysisResults analysisResults,
            bool IsFolder,
            out string destinationPath)
        {
            var sourceFilePath = string.Empty;

            destinationPath = string.Empty;

            try
            {
                if (!File.Exists(storagePathInfoFilePath))
                {
                    const string msg = "StoragePathInfo file not found";
                    LogError(msg + ": " + storagePathInfoFilePath);
                    mMessage = msg;
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
                    const string msg = "StoragePathInfo file was empty";
                    LogError(msg + ": " + storagePathInfoFilePath);
                    mMessage = msg;
                    return false;
                }

                destinationPath = Path.Combine(mWorkDir, Path.GetFileName(sourceFilePath));

                if (IsFolder)
                {
                    analysisResults.CopyDirectory(sourceFilePath, destinationPath, overwrite: true);
                }
                else
                {
                    analysisResults.CopyFileWithRetry(sourceFilePath, destinationPath, overwrite: true);
                }
            }
            catch (Exception ex)
            {
                LogError("Error in RetrieveStoragePathInfoTargetFile", ex);
                return false;
            }

            return true;
        }

        [Obsolete("No longer used")]
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

            mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            if (mDebugLevel >= 1)
            {
                LogMessage("Running PrideConverter on " + Path.GetFileName(msgfResultsFilePath));
            }

            mStatusTools.CurrentOperation = "Running PrideConverter";
            mStatusTools.UpdateAndWrite(mProgress);

            var arguments = "-jar " + PossiblyQuotePath(mPrideConverterProgLoc) +
                            " -converter" +
                            " -mode convert" +
                            " -engine msgf" +
                            " -sourcefile " + PossiblyQuotePath(msgfResultsFilePath) +     // QC_Shew_12_02_Run-03_18Jul12_Roc_12-04-08.msgf
                            " -spectrafile " + PossiblyQuotePath(mzXMLFilePath) +          // QC_Shew_12_02_Run-03_18Jul12_Roc_12-04-08.mzXML
                            " -reportfile " + PossiblyQuotePath(prideReportFilePath) +     // QC_Shew_12_02_Run-03_18Jul12_Roc_12-04-08.msgf-report.xml
                            " -reportOnlyIdentifiedSpectra" +
                            " -debug";

            LogDebug(mJavaProgLoc + " " + arguments);

            mCmdRunner.CreateNoWindow = false;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = false;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, PRIDEConverter_CONSOLE_OUTPUT);
            mCmdRunner.WorkDir = mWorkDir;

            var success = mCmdRunner.RunProgram(mJavaProgLoc, arguments, "PrideConverter", true);

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

        private void StoreInstrumentInfo(DataPackageDatasetInfo datasetInfo)
        {
            StoreInstrumentInfo(datasetInfo.InstrumentGroup, datasetInfo.Instrument);
        }

        private void StoreInstrumentInfo(DataPackageJobInfo dataPkgJob)
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

        private void StoreMzIdSampleInfo(string mzIdFilePath, SampleMetadata sampleMetadata)
        {
            var mzIdFile = new FileInfo(mzIdFilePath);

            if (!mMzIdSampleInfo.ContainsKey(mzIdFile.Name))
            {
                mMzIdSampleInfo.Add(mzIdFile.Name, sampleMetadata);
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            var toolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo>();

            // Deprecated:
            //if (mCreatePrideXMLFiles)
            //{
            //    var prideConverter = new FileInfo(mPrideConverterProgLoc);
            //    if (!prideConverter.Exists)
            //    {
            //        try
            //        {
            //            toolVersionInfo = "Unknown";
            //            return SetStepTaskToolVersion(toolVersionInfo, new List<FileInfo>());
            //        }
            //        catch (Exception ex)
            //        {
            //            var msg = "Exception calling SetStepTaskToolVersion: " + ex.Message;
            //            LogError(msg);
            //            return false;
            //        }
            //    }

            //    // Run the PRIDE Converter using the -version switch to determine its version
            //    toolVersionInfo = GetPrideConverterVersion(prideConverter.FullName);

            //    toolFiles.Add(prideConverter);
            //}
            //else
            //{

            // Lookup the version of the AnalysisManagerPrideConverter plugin
            if (!StoreToolVersionInfoForLoadedAssembly(ref toolVersionInfo, "AnalysisManagerPRIDEConverterPlugIn", includeRevision: false))
            {
                return false;
            }

            toolFiles.Add(new FileInfo(mMSXmlGeneratorAppPath));

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        private void TransferPreviousDatasetFiles(AnalysisResults analysisResults, string remoteTransferDirectory)
        {
            // Delete the dataset files for the previous dataset
            var filesToRetry = new List<string>();

            if (mPreviousDatasetFilesToCopy.Count > 0)
            {
                filesToRetry.Clear();

                try
                {
                    // Copy the files we want to keep to the remote Transfer Directory
                    foreach (var sourceFilePath in mPreviousDatasetFilesToCopy)
                    {
                        if (string.IsNullOrWhiteSpace(sourceFilePath))
                            continue;

                        var targetFilePath = Path.Combine(remoteTransferDirectory, Path.GetFileName(sourceFilePath));

                        if (!File.Exists(sourceFilePath))
                            continue;

                        if (string.Equals(sourceFilePath, targetFilePath, StringComparison.OrdinalIgnoreCase))
                            continue;

                        try
                        {
                            analysisResults.CopyFileWithRetry(sourceFilePath, targetFilePath, true);
                            AddToListIfNew(mPreviousDatasetFilesToDelete, sourceFilePath);
                        }
                        catch (Exception ex)
                        {
                            LogError("Exception copying file to transfer directory", ex);
                            filesToRetry.Add(sourceFilePath);
                        }
                    }
                }
                catch (Exception ex)
                {
                    // Folder creation error
                    LogError("Exception copying files to " + remoteTransferDirectory, ex);
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

        [Obsolete("No longer used")]
        private MSGFReportXMLFileLocations UpdateMSGFReportXMLFileLocation(MSGFReportXMLFileLocations fileLocation, string elementName, bool insideMzDataDescription)
        {
            switch (elementName)
            {
                case "SearchResultIdentifier":
                    fileLocation = MSGFReportXMLFileLocations.SearchResultIdentifier;
                    break;
                case "Metadata":
                    fileLocation = MSGFReportXMLFileLocations.Metadata;
                    break;
                case "Protocol":
                    fileLocation = MSGFReportXMLFileLocations.Protocol;
                    break;
                case "admin":
                    if (insideMzDataDescription)
                    {
                        fileLocation = MSGFReportXMLFileLocations.MzDataAdmin;
                    }
                    break;
                case "instrument":
                    if (insideMzDataDescription)
                    {
                        fileLocation = MSGFReportXMLFileLocations.MzDataInstrument;
                    }
                    break;
                case "dataProcessing":
                    if (insideMzDataDescription)
                    {
                        fileLocation = MSGFReportXMLFileLocations.MzDataDataProcessing;
                    }
                    break;
                case "ExperimentAdditional":
                    fileLocation = MSGFReportXMLFileLocations.ExperimentAdditional;
                    break;
                case "Identifications":
                    fileLocation = MSGFReportXMLFileLocations.Identifications;
                    break;
                case "PTMs":
                    fileLocation = MSGFReportXMLFileLocations.PTMs;
                    break;
                case "DatabaseMappings":
                    fileLocation = MSGFReportXMLFileLocations.DatabaseMappings;
                    break;
                case "ConfigurationOptions":
                    fileLocation = MSGFReportXMLFileLocations.ConfigurationOptions;
                    break;
            }

            return fileLocation;
        }

        /// <summary>
        /// Update the .mzid.gz file for the given job and dataset to have the correct Accession value for FileFormat
        /// Also update attributes location and name for element SpectraData if we converted _dta.txt files to .mgf files
        /// Lastly, remove any empty ModificationParams elements
        /// </summary>
        /// <param name="remoteTransferDirectory">Remote transfer folder</param>
        /// <param name="dataPkgJob">Data package job info</param>
        /// <param name="dataPkgDatasetInfo">Dataset info for this job</param>
        /// <param name="searchedMzML">True if analysis job used a .mzML file (though we track .mzml.gz files with this class)</param>
        /// <param name="mzIdFilePaths">Output: path to the .mzid.gz file for this job (will be multiple files if a SplitFasta search was performed)</param>
        /// <param name="mzIdExistsRemotely">Output: true if the .mzid.gz file already exists in the remote transfer folder</param>
        /// <param name="templateParameters"></param>
        /// <returns>True if success, false if an error</returns>
        private bool UpdateMzIdFiles(
            string remoteTransferDirectory,
            DataPackageJobInfo dataPkgJob,
            DataPackageDatasetInfo dataPkgDatasetInfo,
            bool searchedMzML,
            out List<string> mzIdFilePaths,
            out bool mzIdExistsRemotely,
            IReadOnlyDictionary<string, string> templateParameters)
        {
            var sampleMetadata = new SampleMetadata();
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
            sampleMetadata.InstrumentName = dataPkgJob.Instrument;
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

                var success = UpdateMzIdFile(remoteTransferDirectory, dataPkgJob.Job, dataPkgJob.Dataset, searchedMzML, 0, sampleMetadata, out var mzIdFilePath, out mzIdExistsRemotely);

                if (success)
                {
                    mzIdFilePaths.Add(mzIdFilePath);
                }
                else if (dataPkgJob.NumberOfClonedSteps > 0)
                {
                    mzIdExistsRemotely = false;

                    for (var splitFastaResultID = 1; splitFastaResultID <= dataPkgJob.NumberOfClonedSteps; splitFastaResultID++)
                    {
                        success = UpdateMzIdFile(
                            remoteTransferDirectory,
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

                if (!success)
                {
                    if (string.IsNullOrWhiteSpace(mMessage))
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
        /// <param name="remoteTransferDirectory">Remote transfer folder</param>
        /// <param name="dataPkgJob">Data package job</param>
        /// <param name="dataPkgDataset">Data package dataset</param>
        /// <param name="searchedMzML">True if analysis job used a .mzML file (though we track .mzml.gz files with this class)</param>
        /// <param name="splitFastaResultID">For SplitFasta jobs, the part number being processed; 0 for non-SplitFasta jobs</param>
        /// <param name="sampleMetadata">Sample Metadata</param>
        /// <param name="mzIdFilePath">Output: path to the .mzid.gz file being processed</param>
        /// <param name="mzIdExistsRemotely">Output: true if the .mzid.gz file already exists in the remote transfer folder</param>
        /// <returns>True if success, false if an error</returns>
        private bool UpdateMzIdFile(
            string remoteTransferDirectory,
            int dataPkgJob,
            string dataPkgDataset,
            bool searchedMzML,
            int splitFastaResultID,
            SampleMetadata sampleMetadata,
            out string mzIdFilePath,
            out bool mzIdExistsRemotely)
        {
            var readModAccession = false;
            var readingSpecificityRules = false;

            var attributeOverride = new Dictionary<string, string>();

            var elementCloseDepths = new Stack<int>();

            var fileLocation = MzidXMLFileLocations.Header;
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
                mzIdFilePath = Path.Combine(mWorkDir, sourceFileName);

                if (!File.Exists(mzIdFilePath))
                {
                    // Job-specific version not found locally
                    // If the file already exists in the remote transfer folder, assume that it is up-to-date
                    if (FileExistsInTransferDirectory(remoteTransferDirectory, mzIdFilePath))
                    {
                        LogDebug("Skip updating the .mzid.gz file since already in the transfer folder");
                        mzIdExistsRemotely = true;

                        StoreMzIdSampleInfo(mzIdFilePath, sampleMetadata);

                        return true;
                    }

                    // Look for one that simply starts with the dataset name
                    sourceFileName = dataPkgDataset + "_msgfplus" + filePartText + DOT_MZID_GZ;
                    mzIdFilePath = Path.Combine(mWorkDir, sourceFileName);

                    if (!File.Exists(mzIdFilePath))
                    {
                        if (FileExistsInTransferDirectory(remoteTransferDirectory, mzIdFilePath))
                        {
                            LogDebug("Skip updating the .mzid.gz file since already in the transfer folder");
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
                // Thus, we instead first instantiate a StreamReader using explicit encodings
                // Then instantiate the XmlTextReader

                using (var outFile = new FileStream(updatedMzidFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read))
                using (var zippedOutStream = new GZipStream(outFile, CompressionMode.Compress))
                using (var writer = new XmlTextWriter(zippedOutStream, new UTF8Encoding(false)))
                using (Stream unzippedStream = new GZipStream(new FileStream(mzIdFilePath, FileMode.Open, FileAccess.Read, FileShare.Read), CompressionMode.Decompress))
                using (var sourceFileReader = new StreamReader(unzippedStream, Encoding.GetEncoding("ISO-8859-1")))
                using (var xmlReader = new XmlTextReader(sourceFileReader))
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

                                fileLocation = UpdateMZidXMLFileLocation(fileLocation, xmlReader.Name);

                                var nodeWritten = false;
                                var skipNode = false;

                                attributeOverride.Clear();

                                switch (xmlReader.Name)
                                {
                                    case "SpectraData":
                                        if (searchedMzML)
                                        {
                                            // MS-GF+ will list an .mzML file here
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
                                                spectraDataFilename = dataPkgDataset + AnalysisResources.CDTA_EXTENSION;
                                            }

                                            // The following statement intentionally uses a generic DMS_WorkDir path; do not use mWorkDir
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

                                        if (fileLocation == MzidXMLFileLocations.InputSpectraData && !searchedMzML)
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
                                        if (fileLocation == MzidXMLFileLocations.AnalysisProtocolCollection)
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
                                            var modInfo = ReadWriteCvParam(xmlReader, writer, elementCloseDepths);

                                            if (!string.IsNullOrEmpty(modInfo.Accession))
                                            {
                                                if (!mModificationsUsed.ContainsKey(modInfo.Accession))
                                                {
                                                    mModificationsUsed.Add(modInfo.Accession, modInfo);
                                                }

                                                if (!sampleMetadata.Modifications.ContainsKey(modInfo.Accession))
                                                {
                                                    sampleMetadata.Modifications.Add(modInfo.Accession, modInfo);
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
                                        // Skip this element (and any children nodes enclosed in this element)
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

                PRISM.AppUtils.GarbageCollectNow();

                mzIdExistsRemotely = false;

                if (!replaceOriginal)
                {
                    // Nothing was changed; delete the .tmp file
                    updatedMzidFile.Delete();

                    if (JobFileRenameRequired(dataPkgJob))
                    {
                        mzIdFilePath = Path.Combine(mWorkDir, dataPkgDataset + "_Job" + dataPkgJob + "_msgfplus" + filePartText + DOT_MZID_GZ);
                        sourceMzidFile.MoveTo(mzIdFilePath);
                    }
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
                        mzIdFilePath = Path.Combine(mWorkDir, dataPkgDataset + "_Job" + dataPkgJob + "_msgfplus" + filePartText + DOT_MZID_GZ);
                    }
                    else
                    {
                        mzIdFilePath = Path.Combine(mWorkDir, dataPkgDataset + "_msgfplus" + filePartText + DOT_MZID_GZ);
                    }

                    updatedMzidFile.MoveTo(mzIdFilePath);

                    return true;
                }
                catch (Exception ex)
                {
                    LogError("Exception replacing the original .mzid.gz file with the updated one for job " + dataPkgJob + ", dataset " + dataPkgDataset, ex);
                    return false;
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in UpdateMzIdFile for job " + dataPkgJob + ", dataset " + dataPkgDataset, ex);

                var recentElementNames = string.Empty;

                foreach (var item in recentElements)
                {
                    if (string.IsNullOrEmpty(recentElementNames))
                    {
                        recentElementNames = item;
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

        private MzidXMLFileLocations UpdateMZidXMLFileLocation(MzidXMLFileLocations fileLocation, string elementName)
        {
            return elementName switch
            {
                "SequenceCollection" => MzidXMLFileLocations.SequenceCollection,
                "AnalysisCollection" => MzidXMLFileLocations.AnalysisCollection,
                "AnalysisProtocolCollection" => MzidXMLFileLocations.AnalysisProtocolCollection,
                "DataCollection" => MzidXMLFileLocations.DataCollection,
                "Inputs" => MzidXMLFileLocations.Inputs,
                "SearchDatabase" => MzidXMLFileLocations.InputSearchDatabase,
                "SpectraData" => MzidXMLFileLocations.InputSpectraData,
                "AnalysisData" => MzidXMLFileLocations.AnalysisData,
                _ => fileLocation
            };
        }

        /// <summary>
        /// If the CV param info is enclosed in square brackets, assure that it has exactly three commas
        /// </summary>
        /// <param name="cvParam"></param>
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
        /// <param name="writer"></param>
        /// <param name="type">Parameter type</param>
        /// <param name="value">Value for parameter</param>
        /// <param name="paramsWithCVs">Parameters that should have a CV</param>
        private void WritePXHeader(TextWriter writer, string type, string value, ICollection<string> paramsWithCVs)
        {
            WritePXHeader(writer, type, value, new Dictionary<string, string>(), paramsWithCVs);
        }

        /// <summary>
        /// Append a new header line to the .px file
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="type">Parameter type</param>
        /// <param name="value">Value for parameter</param>
        /// <param name="templateParameters">Dictionary of parameters and values loaded from the template .px file</param>
        /// <param name="paramsWithCVs">Parameters that should have a CV</param>
        /// <param name="minimumValueLength">Minimum length for the parameter value</param>
        private void WritePXHeader(
            TextWriter writer,
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

            WritePXLine(writer, new List<string>
            {
                "MTD",
                type,
                value
            });
        }

        private void WritePXInstruments(TextWriter writer, ICollection<string> paramsWithCVs)
        {
            var accessionsWritten = new SortedSet<string>();

            foreach (var instrumentGroup in mInstrumentGroupsStored)
            {
                var instrumentName = instrumentGroup.Value.FirstOrDefault();

                GetInstrumentAccession(instrumentGroup.Key, instrumentName, out var accession, out var description);

                if (accessionsWritten.Contains(accession))
                    continue;

                accessionsWritten.Add(accession);

                var instrumentCV = GetInstrumentCv(accession, description);
                WritePXHeader(writer, "instrument", instrumentCV, paramsWithCVs);
            }
        }

        private void WritePXLine(TextWriter writer, IReadOnlyCollection<string> items)
        {
            if (items.Count > 0)
            {
                writer.WriteLine(string.Join("\t", items));
            }
        }

        private void WritePXMods(TextWriter writer, ICollection<string> paramsWithCVs)
        {
            if (mModificationsUsed.Count == 0)
            {
                var noPTMsCV = GetCVString("PRIDE", "PRIDE:0000398", "No PTMs are included in the dataset");
                WritePXHeader(writer, "modification", noPTMsCV, paramsWithCVs);
            }
            else
            {
                // Write out each modification, for example, for Unimod:
                //   modification	[UNIMOD,UNIMOD:35,Oxidation,]
                // Or for PSI-mod
                //   modification	[MOD,MOD:00394,acetylated residue,]

                foreach (var item in mModificationsUsed)
                {
                    WritePXHeader(writer, "modification", GetCVString(item.Value), paramsWithCVs);
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

        [Obsolete("No longer used")]
        private bool WriteXMLInstrumentInfo(XmlWriter writer, string instrumentGroup)
        {
            var instrumentDetailsAutoDefined = false;

            var isLCQ = false;
            var isLTQ = false;

            switch (instrumentGroup)
            {
                case "Orbitrap":
                case "VelosOrbi":
                case "QExactive":
                case "QEHFX":
                    instrumentDetailsAutoDefined = true;

                    WriteXMLInstrumentInfoESI(writer, "positive");

                    writer.WriteStartElement("analyzerList");
                    writer.WriteAttributeString("count", "2");

                    WriteXMLInstrumentInfoAnalyzer(writer, "MS", "MS:1000083", "radial ejection linear ion trap");
                    WriteXMLInstrumentInfoAnalyzer(writer, "MS", "MS:1000484", "orbitrap");

                    writer.WriteEndElement();   // analyzerList

                    WriteXMLInstrumentInfoDetector(writer, "MS", "MS:1000624", "inductive detector");
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

                    WriteXMLInstrumentInfoESI(writer, "positive");

                    writer.WriteStartElement("analyzerList");
                    writer.WriteAttributeString("count", "2");

                    WriteXMLInstrumentInfoAnalyzer(writer, "MS", "MS:1000083", "radial ejection linear ion trap");
                    WriteXMLInstrumentInfoAnalyzer(writer, "MS", "MS:1000079", "fourier transform ion cyclotron resonance mass spectrometer");

                    writer.WriteEndElement();   // analyzerList

                    WriteXMLInstrumentInfoDetector(writer, "MS", "MS:1000624", "inductive detector");
                    break;

                case "Exactive":
                    instrumentDetailsAutoDefined = true;

                    WriteXMLInstrumentInfoESI(writer, "positive");

                    writer.WriteStartElement("analyzerList");
                    writer.WriteAttributeString("count", "1");

                    WriteXMLInstrumentInfoAnalyzer(writer, "MS", "MS:1000484", "orbitrap");

                    writer.WriteEndElement();   // analyzerList

                    WriteXMLInstrumentInfoDetector(writer, "MS", "MS:1000624", "inductive detector");
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

            if (isLTQ || isLCQ)
            {
                instrumentDetailsAutoDefined = true;

                WriteXMLInstrumentInfoESI(writer, "positive");

                writer.WriteStartElement("analyzerList");
                writer.WriteAttributeString("count", "1");

                if (isLCQ)
                {
                    WriteXMLInstrumentInfoAnalyzer(writer, "MS", "MS:1000082", "quadrupole ion trap");
                }
                else
                {
                    WriteXMLInstrumentInfoAnalyzer(writer, "MS", "MS:1000083", "radial ejection linear ion trap");
                }

                writer.WriteEndElement();   // analyzerList

                WriteXMLInstrumentInfoDetector(writer, "MS", "MS:1000347", "dynode");
            }

            return instrumentDetailsAutoDefined;
        }

        private void WriteXMLInstrumentInfoAnalyzer(XmlWriter writer, string cvLabel, string accession, string description)
        {
            writer.WriteStartElement("analyzer");
            WriteCVParam(writer, cvLabel, accession, description, string.Empty);
            writer.WriteEndElement();
        }

        private void WriteXMLInstrumentInfoDetector(XmlWriter writer, string cvLabel, string accession, string description)
        {
            writer.WriteStartElement("detector");
            WriteCVParam(writer, cvLabel, accession, description, string.Empty);
            writer.WriteEndElement();
        }

        private void WriteXMLInstrumentInfoESI(XmlWriter writer, string polarity)
        {
            if (string.IsNullOrEmpty(polarity))
                polarity = "positive";

            writer.WriteStartElement("source");
            WriteCVParam(writer, "MS", "MS:1000073", "electrospray ionization", string.Empty);
            WriteCVParam(writer, "MS", "MS:1000037", "polarity", polarity);
            writer.WriteEndElement();
        }

        private DateTime mLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        [Obsolete("No longer used")]
        private void CmdRunner_LoopWaiting()
        {
            if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= 15)
            {
                mLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(mWorkDir, PRIDEConverter_CONSOLE_OUTPUT));

                LogProgress("PRIDEConverter");
            }
        }

        private void MSXmlCreator_LoopWaiting()
        {
            UpdateStatusFile();

            LogProgress("MSXmlCreator (PRIDEConverter)");
        }

        private void MyEMSLDatasetListInfo_FileDownloadedEvent(object sender, FileDownloadedEventArgs e)
        {
            if (e.UnzipRequired)
            {
                foreach (var unzippedFile in mMyEMSLUtilities.MostRecentUnzippedFiles)
                {
                    AddToListIfNew(mPreviousDatasetFilesToDelete, unzippedFile.Value);
                }
            }

            AddToListIfNew(mPreviousDatasetFilesToDelete, Path.Combine(e.DownloadDirectoryPath, e.ArchivedFile.Filename));
        }
    }
}
