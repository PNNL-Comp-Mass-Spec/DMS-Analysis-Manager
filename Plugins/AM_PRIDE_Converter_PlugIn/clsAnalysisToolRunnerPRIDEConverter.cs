using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Xml;
using AnalysisManagerBase;
using MyEMSLReader;
using PHRPReader;

namespace AnalysisManagerPRIDEConverterPlugIn
{
    /// <summary>
    /// Class for running PRIDEConverter
    /// </summary>
    public class clsAnalysisToolRunnerPRIDEConverter : clsAnalysisToolRunnerBase
    {
        #region "Constants"

        const string DOT_GZ = clsAnalysisResources.DOT_GZ_EXTENSION;
        const string DOT_MZML = clsAnalysisResources.DOT_MZML_EXTENSION;
        const string DOT_MZML_GZ = clsAnalysisResources.DOT_MZML_EXTENSION + clsAnalysisResources.DOT_GZ_EXTENSION;

        #endregion

        #region "Module Variables"

        private const string PRIDEConverter_CONSOLE_OUTPUT = "PRIDEConverter_ConsoleOutput.txt";
        public const float PROGRESS_PCT_TOOL_RUNNER_STARTING = 20;
        private const float PROGRESS_PCT_SAVING_RESULTS = 95;
        private const float PROGRESS_PCT_COMPLETE = 99;

        private const string FILE_EXTENSION_PSEUDO_MSGF = ".msgf";
        private const string FILE_EXTENSION_MSGF_REPORT_XML = ".msgf-report.xml";
        private const string FILE_EXTENSION_MSGF_PRIDE_XML = ".msgf-pride.xml";

        private const string PARTIAL_SUBMISSION = "PARTIAL";
        private const string COMPLETE_SUBMISSION = "COMPLETE";

        private const string PNNL_NAME_COUNTRY = "Pacific Northwest National Laboratory, USA";

        private const string DEFAULT_TISSUE_CV = "[BTO, BTO:0000089, blood, ]";
        private const string DEFAULT_CELL_TYPE_CV = "[CL, CL:0000081, blood cell, ]";
        private const string DEFAULT_DISEASE_TYPE_CV = "[DOID, DOID:1612, breast cancer, ]";
        private const string DEFAULT_QUANTIFICATION_TYPE_CV = "[PRIDE, PRIDE:0000436, Spectral counting,]";
        private const string DELETION_WARNING = " -- If you delete this line, assure that the corresponding column values on the SME rows are empty (leave the 'cell_type' and 'disease' column headers on the SMH line, but assure that the SME lines have blank entries for this column)";

        private const double DEFAULT_PVALUE_THRESHOLD = 0.05;

        private string mConsoleOutputErrorMsg;

        // This dictionary tracks the peptide hit jobs defined for this data package
        // The keys are job numbers and the values contains job info
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

        // This list contains full file paths for files that will be deleted from the local work directory
        private List<string> mPreviousDatasetFilesToDelete;

        // This list contains full file paths for files that will be copied from the local work directory to the transfer directory
        private List<string> mPreviousDatasetFilesToCopy;

        private string mCachedOrgDBName = string.Empty;

        // This dictionary holds protein name in the key
        // The value is a key-value pair where the key is the Protein Index and the value is the protein sequence
        private Dictionary<string, KeyValuePair<int, string>> mCachedProteins;

        // This dictionary holds the protein index as the key and tracks the number of filter-passing PSMs for each protein as the value
        private Dictionary<int, int> mCachedProteinPSMCounts;

        // Keys in this dictionary are filenames
        // Values contain info on each file
        // Note that PRIDE uses case-sensitive file names, so it is important to properly capitalize the files to match the official DMS dataset name
        // However, this dictionary is instantiated with a case-insensitive comparer, to prevent duplicate entries
        private Dictionary<string, clsPXFileInfoBase> mPxMasterFileList;

        // Keys in this dictionary are PXFileIDs
        // Values contain info on each file, including the PXFileType and the FileIDs that map to this file (empty list if no mapped files)
        // Note that PRIDE uses case-sensitive file names, so it is important to properly capitalize the files to match the official DMS dataset name
        // However, this dictionary is instantiated with a case-insensitive comparer, to prevent duplicate entries
        private Dictionary<int, clsPXFileInfo> mPxResultFiles;

        private udtFilterThresholdsType mFilterThresholdsUsed;

        // Keys in this dictionary are instrument group names
        // Values are the specific instrument names
        private Dictionary<string, List<string>> mInstrumentGroupsStored;
        private SortedSet<string> mSearchToolsUsed;

        // Keys in this dictionary are NEWT IDs
        // Values are the NEWT name for the given ID
        private Dictionary<int, string> mExperimentNEWTInfo;

        // Keys in this dictionary are Unimod accession names (e.g. UNIMOD:35)
        // Values are CvParam data for the modification
        private Dictionary<string, clsSampleMetadata.udtCvParamInfoType> mModificationsUsed;

        // Keys in this dictionary are mzid.gz file names
        // Values are the sample info for the file
        private Dictionary<string, clsSampleMetadata> mMzIdSampleInfo;

        // Keys in this dictionary are _dta.txt file names
        // Values contain info on each file
        private Dictionary<string, clsPXFileInfoBase> mCDTAFileStats;

        private AnalysisManagerMsXmlGenPlugIn.clsMSXMLCreator mMSXmlCreator;
        private DTAtoMGF.clsDTAtoMGF mDTAtoMGF;

        private clsRunDosProgram mCmdRunner;

        #endregion

        #region "Structures and Enums"

        private struct udtFilterThresholdsType
        {
            public float PValueThreshold;
            public float FDRThreshold;
            public float PepFDRThreshold;
            public float MSGFSpecProbThreshold;
            public bool UseFDRThreshold;
            public bool UsePepFDRThreshold;
            public bool UseMSGFSpecProb;

            public void Clear()
            {
                PValueThreshold = (float) clsAnalysisToolRunnerPRIDEConverter.DEFAULT_PVALUE_THRESHOLD;
                UseFDRThreshold = false;
                UsePepFDRThreshold = false;
                UseMSGFSpecProb = true;
                FDRThreshold = 0.01f;
                PepFDRThreshold = 0.01f;
                MSGFSpecProbThreshold = 1E-09f;
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
            public string MSGFSpecProb;
            public string DeltaScore;
            public string DeltaScoreOther;
            public string Protein;
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
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {
            bool blnSuccess = false;

            try
            {
                // Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (m_DebugLevel > 4)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        "clsAnalysisToolRunnerPRIDEConverter.RunTool(): Enter");
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

                mCacheFolderPath = m_jobParams.GetJobParameter("CacheFolderPath", "\\\\protoapps\\PeptideAtlas_Staging");

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running PRIDEConverter");

                // Initialize dctDataPackageDatasets
                Dictionary<int, clsDataPackageDatasetInfo> dctDataPackageDatasets = null;
                if (!LoadDataPackageDatasetInfo(out dctDataPackageDatasets))
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

                // The objAnalysisResults object is used to copy files to/from this computer
                var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);

                // Assure that the remote transfer folder exists
                var remoteTransferFolder = CreateRemoteTransferFolder(objAnalysisResults, mCacheFolderPath);

                try
                {
                    // Create the remote Transfer Directory
                    if (!Directory.Exists(remoteTransferFolder))
                    {
                        Directory.CreateDirectory(remoteTransferFolder);
                    }
                }
                catch (Exception ex)
                {
                    // Folder creation error
                    LogError("Exception creating transfer directory folder", ex);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Read the PX_Submission_Template.px file
                var dctTemplateParameters = ReadTemplatePXSubmissionFile();

                var jobFailureCount = ProcessJobs(objAnalysisResults, remoteTransferFolder, dctTemplateParameters, dctDataPackageDatasets);

                // Create the PX Submission file
                blnSuccess = CreatePXSubmissionFile(dctTemplateParameters);

                m_progress = PROGRESS_PCT_COMPLETE;
                m_StatusTools.UpdateAndWrite(m_progress);

                if (blnSuccess)
                {
                    if (m_DebugLevel >= 3)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "PRIDEConverter Complete");
                    }
                }

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                if (!UpdateSummaryFile())
                {
                    LogWarning("Error creating summary file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
                }

                // Make sure objects are released
                Thread.Sleep(500);         // 500 msec delay
                PRISM.Processes.clsProgRunner.GarbageCollectNow();

                if (!blnSuccess | jobFailureCount > 0)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                DefineFilesToSkipTransfer();

                var result = MakeResultsFolder();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // MakeResultsFolder handles posting to local log, so set database error message and exit
                    m_message = "Error making results folder";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                result = MoveResultFiles();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    m_message = "Error moving files into results folder";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                result = CopyResultsFolderToServer(mCacheFolderPath);
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    return result;
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in PRIDEConverterPlugin->RunTool", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // No failures so everything must have succeeded
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private int ProcessJobs(clsAnalysisResults objAnalysisResults, string remoteTransferFolder,
            IReadOnlyDictionary<string, string> dctTemplateParameters, Dictionary<int, clsDataPackageDatasetInfo> dctDataPackageDatasets)
        {
            var jobsProcessed = 0;
            var jobFailureCount = 0;

            try
            {
                // Initialize the class-wide variables
                var udtFilterThresholds = InitializeOptions();

                // Extract the dataset raw file paths
                var dctDatasetRawFilePaths = ExtractPackedJobParameterDictionary(clsAnalysisResources.JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS);

                // Process each job in mDataPackagePeptideHitJobs
                // Sort the jobs by dataset so that we can use the same .mzXML file for datasets with multiple jobs
                var linqJobsSortedByDataset = (from item in mDataPackagePeptideHitJobs orderby item.Value.Dataset, SortPreference(item.Value.Tool) select item);

                var assumeInstrumentDataUnpurged = m_jobParams.GetJobParameter("AssumeInstrumentDataUnpurged", true);

                const bool blnContinueOnError = true;
                const int maxErrorCount = 10;
                var dtLastLogTime = DateTime.UtcNow;

                // This dictionary tracks the datasets that have been processed
                // Keys are dataset ID, values are dataset name
                var dctDatasetsProcessed = new Dictionary<int, string>();

                foreach (KeyValuePair<int, clsDataPackageJobInfo> kvJobInfo in linqJobsSortedByDataset)
                {
                    var udtCurrentJobInfo = kvJobInfo.Value;

                    m_StatusTools.CurrentOperation = "Processing job " + udtCurrentJobInfo.Job + ", dataset " + udtCurrentJobInfo.Dataset;

                    Console.WriteLine();
                    Console.WriteLine((jobsProcessed + 1).ToString() + ": " + m_StatusTools.CurrentOperation);

                    var result = ProcessJob(kvJobInfo, udtFilterThresholds, objAnalysisResults, remoteTransferFolder, dctDatasetRawFilePaths,
                        dctTemplateParameters, assumeInstrumentDataUnpurged);

                    if (result != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        jobFailureCount += 1;
                        if (!blnContinueOnError || jobFailureCount > maxErrorCount)
                            break;
                    }

                    if (!dctDatasetsProcessed.ContainsKey(udtCurrentJobInfo.DatasetID))
                    {
                        dctDatasetsProcessed.Add(udtCurrentJobInfo.DatasetID, udtCurrentJobInfo.Dataset);
                    }

                    jobsProcessed += 1;
                    m_progress = ComputeIncrementalProgress(PROGRESS_PCT_TOOL_RUNNER_STARTING, PROGRESS_PCT_SAVING_RESULTS, jobsProcessed,
                        mDataPackagePeptideHitJobs.Count);
                    m_StatusTools.UpdateAndWrite(m_progress);

                    if (DateTime.UtcNow.Subtract(dtLastLogTime).TotalMinutes >= 5 || m_DebugLevel >= 2)
                    {
                        dtLastLogTime = DateTime.UtcNow;
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                            " ... processed " + jobsProcessed + " / " + mDataPackagePeptideHitJobs.Count + " jobs");
                    }
                }

                TransferPreviousDatasetFiles(objAnalysisResults, remoteTransferFolder);

                // Look for datasets associated with the data package that have no PeptideHit jobs
                // Create fake PeptideHit jobs in the .px file to alert the user of the missing jobs

                foreach (var kvDatasetInfo in dctDataPackageDatasets)
                {
                    if (!dctDatasetsProcessed.ContainsKey(kvDatasetInfo.Key))
                    {
                        m_StatusTools.CurrentOperation = "Adding dataset " + kvDatasetInfo.Value.Dataset + " (no associated PeptideHit job)";

                        Console.WriteLine();
                        Console.WriteLine(m_StatusTools.CurrentOperation);

                        AddPlaceholderDatasetEntry(kvDatasetInfo);
                    }
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

        private void AddPlaceholderDatasetEntry(KeyValuePair<int, clsDataPackageDatasetInfo> kvDatasetInfo)
        {
            AddNEWTInfo(kvDatasetInfo.Value.Experiment_NEWT_ID, kvDatasetInfo.Value.Experiment_NEWT_Name);

            // Store the instrument group and instrument name
            StoreInstrumentInfo(kvDatasetInfo.Value);

            var udtDatasetInfo = kvDatasetInfo.Value;
            var strDatasetRawFilePath = Path.Combine(udtDatasetInfo.ServerStoragePath, udtDatasetInfo.Dataset + ".raw");

            var dataPkgJob = clsAnalysisResources.GetPseudoDataPackageJobInfo(udtDatasetInfo);

            var rawFileID = AddPxFileToMasterList(strDatasetRawFilePath, dataPkgJob);

            AddPxResultFile(rawFileID, clsPXFileInfoBase.ePXFileType.Raw, strDatasetRawFilePath, dataPkgJob);
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

        private int AddPxFileToMasterList(string strFilePath, clsDataPackageJobInfo dataPkgJob)
        {
            var fiFile = new FileInfo(strFilePath);

            clsPXFileInfoBase oPXFileInfo = null;
            if (mPxMasterFileList.TryGetValue(fiFile.Name, out oPXFileInfo))
            {
                // File already exists
                return oPXFileInfo.FileID;
            }
            else
            {
                string strFilename = CheckFilenameCase(fiFile, dataPkgJob.Dataset);

                oPXFileInfo = new clsPXFileInfoBase(strFilename, dataPkgJob);

                oPXFileInfo.FileID = mPxMasterFileList.Count + 1;

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
        }

        private bool AddPxResultFile(int intFileID, clsPXFileInfoBase.ePXFileType eFileType, string strFilePath, clsDataPackageJobInfo dataPkgJob)
        {
            var fiFile = new FileInfo(strFilePath);

            clsPXFileInfo oPXFileInfo = null;

            if (mPxResultFiles.TryGetValue(intFileID, out oPXFileInfo))
            {
                // File already defined in the mapping list
                return true;
            }
            else
            {
                clsPXFileInfoBase oMasterPXFileInfo = null;
                if (!mPxMasterFileList.TryGetValue(fiFile.Name, out oMasterPXFileInfo))
                {
                    // File not found in mPxMasterFileList, we cannot add the mapping
                    LogError("File " + fiFile.Name + " not found in mPxMasterFileList; unable to add to mPxResultFiles");
                    return false;
                }

                if (oMasterPXFileInfo.FileID != intFileID)
                {
                    var msg = "FileID mismatch for " + fiFile.Name;
                    LogError(msg + ":  mPxMasterFileList.FileID = " + oMasterPXFileInfo.FileID + " vs. FileID " + intFileID + " passed into AddPxFileToMapping");
                    m_message = msg;
                    return false;
                }

                string strFilename = CheckFilenameCase(fiFile, dataPkgJob.Dataset);

                oPXFileInfo = new clsPXFileInfo(strFilename, dataPkgJob);
                oPXFileInfo.Update(oMasterPXFileInfo);
                oPXFileInfo.PXFileType = eFileType;

                mPxResultFiles.Add(intFileID, oPXFileInfo);

                return true;
            }
        }

        /// <summary>
        /// Adds strValue to lstList only if the value is not yet present in the list
        /// </summary>
        /// <param name="lstList"></param>
        /// <param name="strValue"></param>
        /// <remarks></remarks>
        private void AddToListIfNew(ICollection<string> lstList, string strValue)
        {
            if (!lstList.Contains(strValue))
            {
                lstList.Add(strValue);
            }
        }

        private bool AppendToPXFileInfo(clsDataPackageJobInfo dataPkgJob, IReadOnlyDictionary<string, string> dctDatasetRawFilePaths,
            clsResultFileContainer resultFiles)
        {
            // Add the files to be submitted to ProteomeXchange to the master file list
            // In addition, append new mappings to the ProteomeXchange mapping list

            var intPrideXMLFileID = 0;
            if (!string.IsNullOrEmpty(resultFiles.PrideXmlFilePath))
            {
                AddToListIfNew(mPreviousDatasetFilesToCopy, resultFiles.PrideXmlFilePath);

                intPrideXMLFileID = AddPxFileToMasterList(resultFiles.PrideXmlFilePath, dataPkgJob);
                if (!AddPxResultFile(intPrideXMLFileID, clsPXFileInfoBase.ePXFileType.Result, resultFiles.PrideXmlFilePath, dataPkgJob))
                {
                    return false;
                }
            }

            int rawFileID = 0;
            string strDatasetRawFilePath = string.Empty;
            if (dctDatasetRawFilePaths.TryGetValue(dataPkgJob.Dataset, out strDatasetRawFilePath))
            {
                if (!string.IsNullOrEmpty(strDatasetRawFilePath))
                {
                    rawFileID = AddPxFileToMasterList(strDatasetRawFilePath, dataPkgJob);
                    if (!AddPxResultFile(rawFileID, clsPXFileInfoBase.ePXFileType.Raw, strDatasetRawFilePath, dataPkgJob))
                    {
                        return false;
                    }

                    if (intPrideXMLFileID > 0)
                    {
                        if (!DefinePxFileMapping(intPrideXMLFileID, rawFileID))
                        {
                            return false;
                        }
                    }
                }
            }

            var intPeakfileID = 0;
            if (!string.IsNullOrEmpty(resultFiles.MGFFilePath))
            {
                AddToListIfNew(mPreviousDatasetFilesToCopy, resultFiles.MGFFilePath);

                intPeakfileID = AddPxFileToMasterList(resultFiles.MGFFilePath, dataPkgJob);
                if (!AddPxResultFile(intPeakfileID, clsPXFileInfoBase.ePXFileType.Peak, resultFiles.MGFFilePath, dataPkgJob))
                {
                    return false;
                }

                if (intPrideXMLFileID == 0)
                {
                    // Pride XML file was not created
                    if (rawFileID > 0 && resultFiles.MzIDFilePaths.Count == 0)
                    {
                        // Only associate Peak files with .Raw files if we do not have a .mzid.gz file
                        if (!DefinePxFileMapping(intPeakfileID, rawFileID))
                        {
                            return false;
                        }
                    }
                }
                else
                {
                    // Pride XML file was created
                    if (!DefinePxFileMapping(intPrideXMLFileID, intPeakfileID))
                    {
                        return false;
                    }
                }
            }

            foreach (var mzIdResultFile in resultFiles.MzIDFilePaths)
            {
                var success = AddMzidOrPepXmlFileToPX(dataPkgJob, mzIdResultFile, clsPXFileInfoBase.ePXFileType.ResultMzId, intPrideXMLFileID,
                    rawFileID, intPeakfileID);
                if (!success)
                    return false;
            }

            if (!string.IsNullOrWhiteSpace(resultFiles.PepXMLFile))
            {
                var success = AddMzidOrPepXmlFileToPX(dataPkgJob, resultFiles.PepXMLFile, clsPXFileInfoBase.ePXFileType.Search, intPrideXMLFileID,
                    rawFileID, intPeakfileID);
                if (!success)
                    return false;
            }

            return true;
        }

        private bool AddMzidOrPepXmlFileToPX(clsDataPackageJobInfo dataPkgJob, string resultFilePath, clsPXFileInfoBase.ePXFileType ePxFileType,
            int intPrideXMLFileID, int rawFileID, int intPeakfileID)
        {
            AddToListIfNew(mPreviousDatasetFilesToCopy, resultFilePath);

            var dataFileID = AddPxFileToMasterList(resultFilePath, dataPkgJob);
            if (!AddPxResultFile(dataFileID, ePxFileType, resultFilePath, dataPkgJob))
            {
                return false;
            }

            if (intPrideXMLFileID == 0)
            {
                // Pride XML file was not created
                if (intPeakfileID > 0)
                {
                    if (!DefinePxFileMapping(dataFileID, intPeakfileID))
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
                if (!DefinePxFileMapping(intPrideXMLFileID, dataFileID))
                {
                    return false;
                }
            }

            return true;
        }

        private string CheckFilenameCase(FileInfo fiFile, string strDataset)
        {
            string strFilename = fiFile.Name;

            if (!string.IsNullOrEmpty(fiFile.Extension))
            {
                string strFileBaseName = Path.GetFileNameWithoutExtension(fiFile.Name);

                if (strFileBaseName.ToLower().StartsWith(strDataset.ToLower()))
                {
                    if (!strFileBaseName.StartsWith(strDataset))
                    {
                        // Case-mismatch; fix it
                        if (strFileBaseName.Length == strDataset.Length)
                        {
                            strFileBaseName = strDataset;
                        }
                        else
                        {
                            strFileBaseName = strDataset + strFileBaseName.Substring(strDataset.Length);
                        }
                    }
                }

                if ((fiFile.Extension.Equals(DOT_MZML, StringComparison.InvariantCultureIgnoreCase)))
                {
                    strFilename = strFileBaseName + DOT_MZML;
                }
                else if ((fiFile.Extension.Equals(DOT_MZML_GZ, StringComparison.InvariantCultureIgnoreCase)))
                {
                    strFilename = strFileBaseName + DOT_MZML_GZ;
                }
                else
                {
                    strFilename = strFileBaseName + fiFile.Extension.ToLower();
                }
            }

            return strFilename;
        }

        private double ComputeApproximatePValue(double dblMSGFSpecProb)
        {
            double dblSpecProb = 0;
            double dblPValueEstimate = dblMSGFSpecProb;

            try
            {
                // Estimate Log10(PValue) using 10^(Log10(SpecProb) x 0.9988 + 6.43)
                // This was determined using Job 893431 for dataset QC_Shew_12_02_0pt25_Frac-08_7Nov12_Tiger_12-09-36
                //
                dblPValueEstimate = Math.Log10(dblSpecProb) * 0.9988 + 6.43;
                dblPValueEstimate = Math.Pow(10, dblPValueEstimate);
            }
            catch (Exception ex)
            {
                // Ignore errors here
                // We will simply return strMSGFSpecProb
            }

            return dblPValueEstimate;
        }

        /// <summary>
        /// Convert the _dta.txt file to a .mgf file
        /// </summary>
        /// <param name="dataPkgJob"></param>
        /// <param name="strMGFFilePath">Output parameter: path of the newly created .mgf file</param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool ConvertCDTAToMGF(clsDataPackageJobInfo dataPkgJob, out string strMGFFilePath)
        {
            strMGFFilePath = string.Empty;

            try
            {
                mDTAtoMGF = new DTAtoMGF.clsDTAtoMGF();
                mDTAtoMGF.Combine2And3PlusCharges = false;
                mDTAtoMGF.FilterSpectra = false;
                mDTAtoMGF.MaximumIonsPer100MzInterval = 40;
                mDTAtoMGF.NoMerge = true;
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
                string strMD5Hash = null;
                strMD5Hash = clsGlobal.ComputeFileHashMD5(fiCDTAFile.FullName);

                // Make sure this is either a new _dta.txt file or identical to a previous one
                // Abort processing if the job list contains multiple jobs for the same dataset but those jobs used different _dta.txt files
                // However, if one of the jobs is Sequest and one is MSGF+, preferentially use the _dta.txt file from the MSGF+ job
                clsPXFileInfoBase oFileInfo = null;

                if (mCDTAFileStats.TryGetValue(fiCDTAFile.Name, out oFileInfo))
                {
                    if (oFileInfo.JobInfo.Tool.ToLower().StartsWith("msgf"))
                    {
                        // Existing job found, but it's a MSGF+ job (which is fully supported by PRIDE)
                        // Just use the existing .mgf file
                        return true;
                    }

                    if (fiCDTAFile.Length != oFileInfo.Length)
                    {
                        var msg = "Dataset " + dataPkgJob.Dataset +
                                  " has multiple jobs in this data package, and those jobs used different _dta.txt files; this is not supported";
                        LogError(msg + ": file size mismatch of " + fiCDTAFile.Length + " for job " + dataPkgJob.Job + " vs " + oFileInfo.Length +
                                 " for job " + oFileInfo.JobInfo.Job);
                        m_message = msg;
                        return false;
                    }
                    else if (strMD5Hash != oFileInfo.MD5Hash)
                    {
                        var msg = "Dataset " + dataPkgJob.Dataset +
                                  " has multiple jobs in this data package, and those jobs used different _dta.txt files; this is not supported";
                        LogError(msg + ": MD5 hash mismatch of " + strMD5Hash + " for job " + dataPkgJob.Job + " vs. " + oFileInfo.MD5Hash +
                                 " for job " + oFileInfo.JobInfo.Job);
                        m_message = msg;
                        return false;
                    }

                    // The files match; no point in making a new .mgf file
                    return true;
                }
                else
                {
                    string strFilename = CheckFilenameCase(fiCDTAFile, dataPkgJob.Dataset);

                    oFileInfo = new clsPXFileInfoBase(strFilename, dataPkgJob);

                    // File ID doesn't matter; just use 0
                    oFileInfo.FileID = 0;
                    oFileInfo.Length = fiCDTAFile.Length;
                    oFileInfo.MD5Hash = strMD5Hash;

                    mCDTAFileStats.Add(fiCDTAFile.Name, oFileInfo);
                }

                if (!mDTAtoMGF.ProcessFile(fiCDTAFile.FullName))
                {
                    var msg = "Error converting " + fiCDTAFile.Name + " to a .mgf file for job " + dataPkgJob.Job;
                    LogError(msg + ": " + mDTAtoMGF.GetErrorMessage());
                    m_message = msg;
                    return false;
                }
                else
                {
                    // Delete the _dta.txt file
                    try
                    {
                        fiCDTAFile.Delete();
                    }
                    catch (Exception ex)
                    {
                        // Ignore errors here
                    }
                }

                Thread.Sleep(125);
                PRISM.Processes.clsProgRunner.GarbageCollectNow();

                var fiNewMGFFile = new FileInfo(Path.Combine(m_WorkDir, dataPkgJob.Dataset + ".mgf"));

                if (!fiNewMGFFile.Exists)
                {
                    // MGF file was not created
                    var msg = "A .mgf file was not created for the _dta.txt file for job " + dataPkgJob.Job;
                    m_message = msg;
                    LogError(msg + ": " + mDTAtoMGF.GetErrorMessage());
                    return false;
                }

                strMGFFilePath = fiNewMGFFile.FullName;
            }
            catch (Exception ex)
            {
                LogError("Exception in ConvertCDTAToMGF", ex);
                return false;
            }

            return true;
        }

        private void CopyFailedResultsToArchiveFolder()
        {
            string strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath");
            if (string.IsNullOrWhiteSpace(strFailedResultsFolderPath))
                strFailedResultsFolderPath = "??Not Defined??";

            LogWarning("Processing interrupted; copying results to archive folder: " + strFailedResultsFolderPath);

            // Bump up the debug level if less than 2
            if (m_DebugLevel < 2)
                m_DebugLevel = 2;

            // Make sure the PRIDEConverter console output file is retained
            m_jobParams.RemoveResultFileToSkip(PRIDEConverter_CONSOLE_OUTPUT);

            // Skip the .mgf files; no need to put them in the FailedResults folder
            m_jobParams.AddResultFileExtensionToSkip(".mgf");

            // Try to save whatever files are in the work directory
            string strFolderPathToArchive = null;
            strFolderPathToArchive = string.Copy(m_WorkDir);

            // Make the results folder
            var result = MakeResultsFolder();
            if (result == CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Move the result files into the result folder
                result = MoveResultFiles();
                if (result == CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Move was a success; update strFolderPathToArchive
                    strFolderPathToArchive = Path.Combine(m_WorkDir, m_ResFolderName);
                }
            }

            // Copy the results folder to the Archive folder
            var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
            objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive);
        }

        /// <summary>
        /// Counts the number of items of type eFileType in mPxResultFiles
        /// </summary>
        /// <param name="eFileType"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private int CountResultFilesByType(clsPXFileInfoBase.ePXFileType eFileType)
        {
            int intCount = 0;
            intCount = (from item in mPxResultFiles where item.Value.PXFileType == eFileType select item).Count();

            return intCount;
        }

        /// <summary>
        /// Creates (or retrieves) the .mzXML file for this dataset if it does not exist in the working directory
        /// Utilizes dataset info stored in several packed job parameters
        /// Newly created .mzXML files will be copied to the MSXML_Cache folder
        /// </summary>
        /// <returns>True if the file exists or was created</returns>
        /// <remarks></remarks>
        private bool CreateMzXMLFileIfMissing(string strDataset, clsAnalysisResults objAnalysisResults,
            IReadOnlyDictionary<string, string> dctDatasetRawFilePaths)
        {
            bool blnSuccess = false;
            string strDestPath = string.Empty;

            try
            {
                // Look in m_WorkDir for the .mzXML file for this dataset
                var fiMzXmlFilePathLocal = new FileInfo(Path.Combine(m_WorkDir, strDataset + clsAnalysisResources.DOT_MZXML_EXTENSION));

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
                string strMzXmlStoragePathFile = fiMzXmlFilePathLocal.FullName + clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX;

                if (File.Exists(strMzXmlStoragePathFile))
                {
                    blnSuccess = RetrieveStoragePathInfoTargetFile(strMzXmlStoragePathFile, objAnalysisResults, out strDestPath);
                    if (blnSuccess)
                    {
                        AddToListIfNew(mPreviousDatasetFilesToDelete, strDestPath);
                        return true;
                    }
                }

                // Need to create the .mzXML file

                var dctDatasetYearQuarter =
                    ExtractPackedJobParameterDictionary(clsAnalysisResourcesPRIDEConverter.JOB_PARAM_DICTIONARY_DATASET_STORAGE_YEAR_QUARTER);

                if (!dctDatasetRawFilePaths.ContainsKey(strDataset))
                {
                    LogError("Dataset " + strDataset + " not found in job parameter " + clsAnalysisResources.JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS +
                             "; unable to create the missing .mzXML file");
                    return false;
                }

                m_jobParams.AddResultFileToSkip("MSConvert_ConsoleOutput.txt");

                mMSXmlCreator = new AnalysisManagerMsXmlGenPlugIn.clsMSXMLCreator(mMSXmlGeneratorAppPath, m_WorkDir, m_Dataset, m_DebugLevel,
                    m_jobParams);
                RegisterEvents(mMSXmlCreator);
                mMSXmlCreator.LoopWaiting += mMSXmlCreator_LoopWaiting;

                mMSXmlCreator.UpdateDatasetName(strDataset);

                // Make sure the dataset file is present in the working directory
                // Copy it locally if necessary

                var strDatasetFilePathRemote = dctDatasetRawFilePaths[strDataset];

                var blnDatasetFileIsAFolder = Directory.Exists(strDatasetFilePathRemote);

                var strDatasetFilePathLocal = Path.Combine(m_WorkDir, Path.GetFileName(strDatasetFilePathRemote));

                if (blnDatasetFileIsAFolder)
                {
                    // Confirm that the dataset folder exists in the working directory

                    if (!Directory.Exists(strDatasetFilePathLocal))
                    {
                        // Directory not found; look for a storage path info file
                        if (File.Exists(strDatasetFilePathLocal + clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX))
                        {
                            RetrieveStoragePathInfoTargetFile(strDatasetFilePathLocal + clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX,
                                objAnalysisResults, IsFolder: true, strDestPath: out strDestPath);
                        }
                        else
                        {
                            // Copy the dataset folder locally
                            objAnalysisResults.CopyDirectory(strDatasetFilePathRemote, strDatasetFilePathLocal, Overwrite: true);
                        }
                    }
                }
                else
                {
                    // Confirm that the dataset file exists in the working directory
                    if (!File.Exists(strDatasetFilePathLocal))
                    {
                        // File not found; Look for a storage path info file
                        if (File.Exists(strDatasetFilePathLocal + clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX))
                        {
                            RetrieveStoragePathInfoTargetFile(strDatasetFilePathLocal + clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX,
                                objAnalysisResults, out strDestPath);
                            AddToListIfNew(mPreviousDatasetFilesToDelete, strDestPath);
                        }
                        else
                        {
                            // Copy the dataset file locally
                            objAnalysisResults.CopyFileWithRetry(strDatasetFilePathRemote, strDatasetFilePathLocal, Overwrite: true);
                            AddToListIfNew(mPreviousDatasetFilesToDelete, strDatasetFilePathLocal);
                        }
                    }
                    m_jobParams.AddResultFileToSkip(Path.GetFileName(strDatasetFilePathLocal));
                }

                blnSuccess = mMSXmlCreator.CreateMZXMLFile();

                if (!blnSuccess && string.IsNullOrEmpty(m_message))
                {
                    m_message = mMSXmlCreator.ErrorMessage;
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "Unknown error creating the mzXML file for dataset " + strDataset;
                    }
                    else if (!m_message.Contains(strDataset))
                    {
                        m_message += "; dataset " + strDataset;
                    }
                    LogError(m_message);
                }

                if (!blnSuccess)
                    return false;

                fiMzXmlFilePathLocal.Refresh();
                if (fiMzXmlFilePathLocal.Exists)
                {
                    AddToListIfNew(mPreviousDatasetFilesToDelete, fiMzXmlFilePathLocal.FullName);
                }
                else
                {
                    LogError("MSXmlCreator did not create the .mzXML file for dataset " + strDataset);
                    return false;
                }

                // Copy the .mzXML file to the cache

                string strMSXmlGeneratorName = Path.GetFileNameWithoutExtension(mMSXmlGeneratorAppPath);
                string strDatasetYearQuarter = string.Empty;
                if (!dctDatasetYearQuarter.TryGetValue(strDataset, out strDatasetYearQuarter))
                {
                    strDatasetYearQuarter = string.Empty;
                }

                CopyMzXMLFileToServerCache(fiMzXmlFilePathLocal.FullName, strDatasetYearQuarter, strMSXmlGeneratorName, blnPurgeOldFilesIfNeeded: true);

                m_jobParams.AddResultFileToSkip(Path.GetFileName(fiMzXmlFilePathLocal.FullName + clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX));

                Thread.Sleep(250);
                PRISM.Processes.clsProgRunner.GarbageCollectNow();

                try
                {
                    if (blnDatasetFileIsAFolder)
                    {
                        // Delete the local dataset folder
                        if (Directory.Exists(strDatasetFilePathLocal))
                        {
                            Directory.Delete(strDatasetFilePathLocal, true);
                        }
                    }
                    else
                    {
                        // Delete the local dataset file
                        if (File.Exists(strDatasetFilePathLocal))
                        {
                            File.Delete(strDatasetFilePathLocal);
                        }
                    }
                }
                catch (Exception ex)
                {
                    // Ignore errors here
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in CreateMzXMLFileIfMissing", ex);
                return false;
            }

            return blnSuccess;
        }

        private string CreatePseudoMSGFFileUsingPHRPReader(int intJob, string strDataset, udtFilterThresholdsType udtFilterThresholds,
            IDictionary<string, List<udtPseudoMSGFDataType>> lstPseudoMSGFData)
        {
            const int MSGF_SPECPROB_NOTDEFINED = 10;
            const int PVALUE_NOTDEFINED = 10;

            string strPseudoMsgfFilePath = null;

            var blnFDRValuesArePresent = false;
            var blnPepFDRValuesArePresent = false;
            var blnMSGFValuesArePresent = false;

            try
            {
                clsDataPackageJobInfo dataPkgJob = null;

                if (!mDataPackagePeptideHitJobs.TryGetValue(intJob, out dataPkgJob))
                {
                    LogError("Job " + intJob + " not found in mDataPackagePeptideHitJobs; this is unexpected");
                    return string.Empty;
                }

                if (lstPseudoMSGFData.Count > 0)
                {
                    lstPseudoMSGFData.Clear();
                }

                // The .MSGF file can only contain one match for each scan number
                // If it includes multiple matches, then PRIDE Converter crashes when reading the .mzXML file
                // Furthermore, the .msgf-report.xml file cannot have extra entries that are not in the .msgf file
                // Thus, only keep the best-scoring match for each spectrum
                //
                // The keys in each of dctBestMatchByScan and dctBestMatchByScanScoreValues are scan numbers
                // The value for dctBestMatchByScan is a KeyValue pair where the key is the score for this match
                var dctBestMatchByScan = new Dictionary<int, KeyValuePair<double, string>>();
                var dctBestMatchByScanScoreValues = new Dictionary<int, udtPseudoMSGFDataType>();

                var strMzXMLFilename = strDataset + ".mzXML";

                // Determine the correct capitalization for the mzXML file
                var diWorkdir = new DirectoryInfo(m_WorkDir);
                FileInfo[] fiFiles = diWorkdir.GetFiles(strMzXMLFilename);

                if (fiFiles.Length > 0)
                {
                    strMzXMLFilename = fiFiles[0].Name;
                }
                else
                {
                    // mzXML file not found; don't worry about this right now (it's possible that CreateMSGFReportFilesOnly = True)
                }

                var strSynopsisFileName = clsPHRPReader.GetPHRPSynopsisFileName(dataPkgJob.PeptideHitResultType, dataPkgJob.Dataset);

                var strSynopsisFilePath = Path.Combine(m_WorkDir, strSynopsisFileName);

                if (!File.Exists(strSynopsisFilePath))
                {
                    var strSynopsisFilePathAlt = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(strSynopsisFilePath, "Dataset_msgfdb.txt");
                    if (File.Exists(strSynopsisFilePathAlt))
                    {
                        strSynopsisFilePath = strSynopsisFilePathAlt;
                    }
                }

                // Check whether PHRP files with a prefix of "Job12345_" exist
                // This prefix is added by RetrieveDataPackagePeptideHitJobPHRPFiles if multiple peptide_hit jobs are included for the same dataset
                var strSynopsisFilePathWithJob = Path.Combine(m_WorkDir, "Job" + dataPkgJob.Job + "_" + strSynopsisFileName);

                if (File.Exists(strSynopsisFilePathWithJob))
                {
                    strSynopsisFilePath = string.Copy(strSynopsisFilePathWithJob);
                }
                else if (!File.Exists(strSynopsisFilePath))
                {
                    var strSynopsisFilePathAlt = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(strSynopsisFilePathWithJob, "Dataset_msgfdb.txt");
                    if (File.Exists(strSynopsisFilePathAlt))
                    {
                        strSynopsisFilePath = strSynopsisFilePathAlt;
                    }
                }

                using (var objReader = new clsPHRPReader(strSynopsisFilePath, dataPkgJob.PeptideHitResultType, true, true))
                {
                    objReader.SkipDuplicatePSMs = false;

                    // Read the data, filtering on either PepFDR or FDR if defined, or MSGF_SpecProb if PepFDR and/or FDR are not available

                    while (objReader.MoveNext())
                    {
                        var blnValidPSM = true;
                        var blnThresholdChecked = false;

                        var dblMSGFSpecProb = Convert.ToDouble(MSGF_SPECPROB_NOTDEFINED);
                        var dblFDR = Convert.ToDouble(-1);
                        var dblPepFDR = Convert.ToDouble(-1);
                        var dblPValue = Convert.ToDouble(PVALUE_NOTDEFINED);
                        var dblScoreForCurrentMatch = Convert.ToDouble(100);

                        // Determine MSGFSpecProb; store 10 if we don't find a valid number
                        if (!double.TryParse(objReader.CurrentPSM.MSGFSpecProb, out dblMSGFSpecProb))
                        {
                            dblMSGFSpecProb = MSGF_SPECPROB_NOTDEFINED;
                        }

                        switch (dataPkgJob.PeptideHitResultType)
                        {
                            case clsPHRPReader.ePeptideHitResultType.Sequest:
                                if (dblMSGFSpecProb < MSGF_SPECPROB_NOTDEFINED)
                                {
                                    dblPValue = ComputeApproximatePValue(dblMSGFSpecProb);
                                    dblScoreForCurrentMatch = dblMSGFSpecProb;
                                    blnMSGFValuesArePresent = true;
                                }
                                else
                                {
                                    if (blnMSGFValuesArePresent)
                                    {
                                        // Skip this result; it had a score value too low to be processed with MSGF
                                        dblPValue = 1;
                                        blnValidPSM = false;
                                    }
                                    else
                                    {
                                        dblPValue = 0.025;
                                        // Note: storing 1000-XCorr so that lower values will be considered higher confidence
                                        dblScoreForCurrentMatch = 1000 - (objReader.CurrentPSM.GetScoreDbl(clsPHRPParserSequest.DATA_COLUMN_XCorr, 1));
                                    }
                                }

                                break;
                            case clsPHRPReader.ePeptideHitResultType.XTandem:
                                if (dblMSGFSpecProb < MSGF_SPECPROB_NOTDEFINED)
                                {
                                    dblPValue = ComputeApproximatePValue(dblMSGFSpecProb);
                                    dblScoreForCurrentMatch = dblMSGFSpecProb;
                                    blnMSGFValuesArePresent = true;
                                }
                                else
                                {
                                    if (blnMSGFValuesArePresent)
                                    {
                                        // Skip this result; it had a score value too low to be processed with MSGF
                                        dblPValue = 1;
                                        blnValidPSM = false;
                                    }
                                    else
                                    {
                                        dblPValue = 0.025;
                                        dblScoreForCurrentMatch = 1000 + objReader.CurrentPSM.GetScoreDbl(clsPHRPParserXTandem.DATA_COLUMN_Peptide_Expectation_Value_LogE, 1);
                                    }
                                }

                                break;
                            case clsPHRPReader.ePeptideHitResultType.Inspect:
                                dblPValue = objReader.CurrentPSM.GetScoreDbl(clsPHRPParserInspect.DATA_COLUMN_PValue, PVALUE_NOTDEFINED);

                                if (dblMSGFSpecProb < MSGF_SPECPROB_NOTDEFINED)
                                {
                                    dblScoreForCurrentMatch = dblMSGFSpecProb;
                                }
                                else
                                {
                                    if (blnMSGFValuesArePresent)
                                    {
                                        // Skip this result; it had a score value too low to be processed with MSGF
                                        dblPValue = 1;
                                        blnValidPSM = false;
                                    }
                                    else
                                    {
                                        // Note: storing 1000-TotalPRMScore so that lower values will be considered higher confidence
                                        dblScoreForCurrentMatch = 1000 - (objReader.CurrentPSM.GetScoreDbl(clsPHRPParserInspect.DATA_COLUMN_TotalPRMScore, 1));
                                    }
                                }

                                break;
                            case clsPHRPReader.ePeptideHitResultType.MSGFDB:
                                dblFDR = objReader.CurrentPSM.GetScoreDbl(clsPHRPParserMSGFDB.DATA_COLUMN_FDR, -1);
                                if (dblFDR > -1)
                                {
                                    blnFDRValuesArePresent = true;
                                }

                                dblPepFDR = objReader.CurrentPSM.GetScoreDbl(clsPHRPParserMSGFDB.DATA_COLUMN_PepFDR, -1);
                                if (dblPepFDR > -1)
                                {
                                    blnPepFDRValuesArePresent = true;
                                }

                                dblPValue = objReader.CurrentPSM.GetScoreDbl(clsPHRPParserMSGFDB.DATA_COLUMN_PValue, PVALUE_NOTDEFINED);
                                dblScoreForCurrentMatch = dblMSGFSpecProb;
                                break;
                        }

                        if (udtFilterThresholds.UseMSGFSpecProb)
                        {
                            if (dblMSGFSpecProb > udtFilterThresholds.MSGFSpecProbThreshold)
                            {
                                blnValidPSM = false;
                            }
                            blnThresholdChecked = true;

                            if (!mFilterThresholdsUsed.UseMSGFSpecProb)
                            {
                                mFilterThresholdsUsed.UseMSGFSpecProb = true;
                                mFilterThresholdsUsed.MSGFSpecProbThreshold = udtFilterThresholds.MSGFSpecProbThreshold;
                            }
                        }

                        if (blnPepFDRValuesArePresent && udtFilterThresholds.UsePepFDRThreshold)
                        {
                            // Typically only MSGFDB results will have PepFDR values
                            if (dblPepFDR > udtFilterThresholds.PepFDRThreshold)
                            {
                                blnValidPSM = false;
                            }
                            blnThresholdChecked = true;

                            if (!mFilterThresholdsUsed.UsePepFDRThreshold)
                            {
                                mFilterThresholdsUsed.UsePepFDRThreshold = true;
                                mFilterThresholdsUsed.PepFDRThreshold = udtFilterThresholds.PepFDRThreshold;
                            }
                        }

                        if (blnFDRValuesArePresent && udtFilterThresholds.UseFDRThreshold)
                        {
                            // Typically only MSGFDB results will have FDR values
                            if (dblFDR > udtFilterThresholds.FDRThreshold)
                            {
                                blnValidPSM = false;
                            }
                            blnThresholdChecked = true;

                            if (!mFilterThresholdsUsed.UseFDRThreshold)
                            {
                                mFilterThresholdsUsed.UseFDRThreshold = true;
                                mFilterThresholdsUsed.FDRThreshold = udtFilterThresholds.FDRThreshold;
                            }
                        }

                        if (blnValidPSM & !blnThresholdChecked)
                        {
                            // Switch to filtering on MSGFSpecProbThreshold instead of on FDR or PepFDR
                            if (dblMSGFSpecProb < MSGF_SPECPROB_NOTDEFINED && udtFilterThresholds.MSGFSpecProbThreshold < 0.0001)
                            {
                                if (dblMSGFSpecProb > udtFilterThresholds.MSGFSpecProbThreshold)
                                {
                                    blnValidPSM = false;
                                }

                                if (!mFilterThresholdsUsed.UseMSGFSpecProb)
                                {
                                    mFilterThresholdsUsed.UseMSGFSpecProb = true;
                                    mFilterThresholdsUsed.MSGFSpecProbThreshold = udtFilterThresholds.MSGFSpecProbThreshold;
                                }
                            }
                        }

                        if (blnValidPSM)
                        {
                            // Filter on P-value
                            if (dblPValue >= udtFilterThresholds.PValueThreshold)
                            {
                                blnValidPSM = false;
                            }
                        }

                        if (blnValidPSM)
                        {
                            // Determine the protein index in mCachedProteins

                            KeyValuePair<int, string> kvIndexAndSequence;

                            if (!mCachedProteins.TryGetValue(objReader.CurrentPSM.ProteinFirst, out kvIndexAndSequence))
                            {
                                // Protein not found in mCachedProteins
                                // If the search engine is MSGFDB and the protein name starts with REV_ or XXX_ then skip this protein since it's a decoy result
                                // Otherwise, add the protein to mCachedProteins and mCachedProteinPSMCounts, though we won't know its sequence

                                string strProteinUCase = objReader.CurrentPSM.ProteinFirst.ToUpper();

                                if (dataPkgJob.PeptideHitResultType == clsPHRPReader.ePeptideHitResultType.MSGFDB)
                                {
                                    if (strProteinUCase.StartsWith("REV_") || strProteinUCase.StartsWith("XXX_"))
                                    {
                                        blnValidPSM = false;
                                    }
                                }
                                else
                                {
                                    if (strProteinUCase.StartsWith("REVERSED_") || strProteinUCase.StartsWith("SCRAMBLED_") ||
                                        strProteinUCase.StartsWith("XXX."))
                                    {
                                        blnValidPSM = false;
                                    }
                                }

                                if (blnValidPSM)
                                {
                                    kvIndexAndSequence = new KeyValuePair<int, string>(mCachedProteins.Count, string.Empty);
                                    mCachedProteinPSMCounts.Add(kvIndexAndSequence.Key, 0);
                                    mCachedProteins.Add(objReader.CurrentPSM.ProteinFirst, kvIndexAndSequence);
                                }
                            }
                        }

                        if (blnValidPSM)
                        {
                            // These fields are used to hold different scores depending on the search engine
                            var strTotalPRMScore = "0";
                            var strPValue = "0";
                            var strDeltaScore = "0";
                            var strDeltaScoreOther = "0";

                            switch (dataPkgJob.PeptideHitResultType)
                            {
                                case clsPHRPReader.ePeptideHitResultType.Sequest:
                                    strTotalPRMScore = objReader.CurrentPSM.GetScore(clsPHRPParserSequest.DATA_COLUMN_Sp);
                                    strPValue = dblPValue.ToString("0.00");
                                    strDeltaScore = objReader.CurrentPSM.GetScore(clsPHRPParserSequest.DATA_COLUMN_DelCn);
                                    strDeltaScoreOther = objReader.CurrentPSM.GetScore(clsPHRPParserSequest.DATA_COLUMN_DelCn2);

                                    break;
                                case clsPHRPReader.ePeptideHitResultType.XTandem:
                                    strTotalPRMScore = objReader.CurrentPSM.GetScore(clsPHRPParserXTandem.DATA_COLUMN_Peptide_Hyperscore);
                                    strPValue = dblPValue.ToString("0.00");
                                    strDeltaScore = objReader.CurrentPSM.GetScore(clsPHRPParserXTandem.DATA_COLUMN_DeltaCn2);

                                    break;
                                case clsPHRPReader.ePeptideHitResultType.Inspect:
                                    strTotalPRMScore = objReader.CurrentPSM.GetScore(clsPHRPParserInspect.DATA_COLUMN_TotalPRMScore);
                                    strPValue = objReader.CurrentPSM.GetScore(clsPHRPParserInspect.DATA_COLUMN_PValue);
                                    strDeltaScore = objReader.CurrentPSM.GetScore(clsPHRPParserInspect.DATA_COLUMN_DeltaScore);
                                    strDeltaScoreOther = objReader.CurrentPSM.GetScore(clsPHRPParserInspect.DATA_COLUMN_DeltaScoreOther);

                                    break;
                                case clsPHRPReader.ePeptideHitResultType.MSGFDB:
                                    strTotalPRMScore = objReader.CurrentPSM.GetScore(clsPHRPParserMSGFDB.DATA_COLUMN_DeNovoScore);
                                    strPValue = objReader.CurrentPSM.GetScore(clsPHRPParserMSGFDB.DATA_COLUMN_PValue);

                                    break;
                            }

                            // Construct the text that we would write to the pseudo MSGF file
                            string strMSGFText = null;
                            strMSGFText = strMzXMLFilename + "\t" + objReader.CurrentPSM.ScanNumber + "\t" + objReader.CurrentPSM.Peptide + "\t" +
                                          objReader.CurrentPSM.ProteinFirst + "\t" + objReader.CurrentPSM.Charge + "\t" +
                                          objReader.CurrentPSM.MSGFSpecProb + "\t" + objReader.CurrentPSM.PeptideCleanSequence.Length + "\t" +
                                          strTotalPRMScore + "\t" + "0\t" + "0\t" + "0\t" + "0\t" + objReader.CurrentPSM.NumTrypticTerminii + "\t" +
                                          strPValue + "\t" + "0\t" + strDeltaScore + "\t" + strDeltaScoreOther + "\t" + objReader.CurrentPSM.ResultID +
                                          "\t" + "0\t" + "0\t" + objReader.CurrentPSM.MSGFSpecProb;

                            // Add or update dctBestMatchByScan and dctBestMatchByScanScoreValues
                            KeyValuePair<double, string> kvBestMatchForScan;
                            bool blnNewScanNumber = false;

                            if (dctBestMatchByScan.TryGetValue(objReader.CurrentPSM.ScanNumber, out kvBestMatchForScan))
                            {
                                if (dblScoreForCurrentMatch >= kvBestMatchForScan.Key)
                                {
                                    // Skip this result since it has a lower score than the match already stored in dctBestMatchByScan
                                    blnValidPSM = false;
                                }
                                else
                                {
                                    // Update dctBestMatchByScan
                                    dctBestMatchByScan[objReader.CurrentPSM.ScanNumber] = new KeyValuePair<double, string>(dblScoreForCurrentMatch, strMSGFText);
                                    blnValidPSM = true;
                                }
                                blnNewScanNumber = false;
                            }
                            else
                            {
                                // Scan not yet present in dctBestMatchByScan; add it
                                kvBestMatchForScan = new KeyValuePair<double, string>(dblScoreForCurrentMatch, strMSGFText);
                                dctBestMatchByScan.Add(objReader.CurrentPSM.ScanNumber, kvBestMatchForScan);
                                blnValidPSM = true;
                                blnNewScanNumber = true;
                            }

                            if (blnValidPSM)
                            {
                                string strPrefix = string.Empty;
                                string strSuffix = string.Empty;
                                string strPrimarySequence = string.Empty;

                                if (!clsPeptideCleavageStateCalculator.SplitPrefixAndSuffixFromSequence(objReader.CurrentPSM.Peptide, out strPrimarySequence, out strPrefix, out strSuffix))
                                {
                                    strPrefix = string.Empty;
                                    strSuffix = string.Empty;
                                }

                                var udtPseudoMSGFData = new udtPseudoMSGFDataType();
                                udtPseudoMSGFData.ResultID = objReader.CurrentPSM.ResultID;
                                udtPseudoMSGFData.Peptide = string.Copy(objReader.CurrentPSM.Peptide);
                                udtPseudoMSGFData.CleanSequence = string.Copy(objReader.CurrentPSM.PeptideCleanSequence);
                                udtPseudoMSGFData.PrefixResidue = string.Copy(strPrefix);
                                udtPseudoMSGFData.SuffixResidue = string.Copy(strSuffix);
                                udtPseudoMSGFData.ScanNumber = objReader.CurrentPSM.ScanNumber;
                                udtPseudoMSGFData.ChargeState = objReader.CurrentPSM.Charge;
                                udtPseudoMSGFData.PValue = string.Copy(strPValue);
                                udtPseudoMSGFData.MQScore = string.Copy(objReader.CurrentPSM.MSGFSpecProb);
                                udtPseudoMSGFData.TotalPRMScore = string.Copy(strTotalPRMScore);
                                udtPseudoMSGFData.NTT = objReader.CurrentPSM.NumTrypticTerminii;
                                udtPseudoMSGFData.MSGFSpecProb = string.Copy(objReader.CurrentPSM.MSGFSpecProb);
                                udtPseudoMSGFData.DeltaScore = string.Copy(strDeltaScore);
                                udtPseudoMSGFData.DeltaScoreOther = string.Copy(strDeltaScoreOther);
                                udtPseudoMSGFData.Protein = objReader.CurrentPSM.ProteinFirst;

                                if (blnNewScanNumber)
                                {
                                    dctBestMatchByScanScoreValues.Add(objReader.CurrentPSM.ScanNumber, udtPseudoMSGFData);
                                }
                                else
                                {
                                    dctBestMatchByScanScoreValues[objReader.CurrentPSM.ScanNumber] = udtPseudoMSGFData;
                                }
                            }
                        }
                    }
                }

                if (JobFileRenameRequired(intJob))
                {
                    strPseudoMsgfFilePath = Path.Combine(m_WorkDir, dataPkgJob.Dataset + "_Job" + dataPkgJob.Job.ToString() + FILE_EXTENSION_PSEUDO_MSGF);
                }
                else
                {
                    strPseudoMsgfFilePath = Path.Combine(m_WorkDir, dataPkgJob.Dataset + FILE_EXTENSION_PSEUDO_MSGF);
                }

                using (var swMSGFFile = new StreamWriter(new FileStream(strPseudoMsgfFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    // Write the header line
                    swMSGFFile.WriteLine("#SpectrumFile\t" + "Scan#\t" + "Annotation\t" + "Protein\t" + "Charge\t" + "MQScore\t" + "Length\t" +
                                         "TotalPRMScore\t" + "MedianPRMScore\t" + "FractionY\t" + "FractionB\t" + "Intensity\t" + "NTT\t" +
                                         "p-value\t" + "F-Score\t" + "DeltaScore\t" + "DeltaScoreOther\t" + "RecordNumber\t" + "DBFilePos\t" +
                                         "SpecFilePos\t" + "SpecProb");

                    // Write out the filter-passing matches to the pseudo MSGF text file
                    foreach (KeyValuePair<int, KeyValuePair<double, string>> kvItem in dctBestMatchByScan)
                    {
                        swMSGFFile.WriteLine(kvItem.Value.Value);
                    }
                }

                // Store the filter-passing matches in lstPseudoMSGFData

                foreach (KeyValuePair<int, udtPseudoMSGFDataType> kvItem in dctBestMatchByScanScoreValues)
                {
                    List<udtPseudoMSGFDataType> lstMatchesForProtein = null;
                    if (lstPseudoMSGFData.TryGetValue(kvItem.Value.Protein, out lstMatchesForProtein))
                    {
                        lstMatchesForProtein.Add(kvItem.Value);
                    }
                    else
                    {
                        lstMatchesForProtein = new List<udtPseudoMSGFDataType>();
                        lstMatchesForProtein.Add(kvItem.Value);
                        lstPseudoMSGFData.Add(kvItem.Value.Protein, lstMatchesForProtein);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in CreatePseudoMSGFFileUsingPHRPReader", ex);
                return string.Empty;
            }

            return strPseudoMsgfFilePath;
        }

        /// <summary>
        /// Create the .msgf-report.xml file
        /// </summary>
        /// <param name="intJob"></param>
        /// <param name="strDataset"></param>
        /// <param name="udtFilterThresholds"></param>
        /// <param name="strPrideReportXMLFilePath">Output parameter: the full path of the newly created .msgf-report.xml file</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        private bool CreateMSGFReportFile(int intJob, string strDataset, udtFilterThresholdsType udtFilterThresholds,
            out string strPrideReportXMLFilePath)
        {
            bool blnSuccess = false;

            string strTemplateFileName = null;
            string strPseudoMsgfFilePath = null;

            string strLocalOrgDBFolder = m_mgrParams.GetParam("orgdbdir");
            string strOrgDBNameGenerated = null;
            string strProteinCollectionListOrFasta = null;

            var lstPseudoMSGFData = new Dictionary<string, List<udtPseudoMSGFDataType>>();

            strPrideReportXMLFilePath = string.Empty;

            try
            {
                strTemplateFileName = clsAnalysisResourcesPRIDEConverter.GetMSGFReportTemplateFilename(m_jobParams, WarnIfJobParamMissing: false);

                strOrgDBNameGenerated = m_jobParams.GetJobParameter("PeptideSearch",
                    clsAnalysisResourcesPRIDEConverter.GetGeneratedFastaParamNameForJob(intJob), string.Empty);
                if (string.IsNullOrEmpty(strOrgDBNameGenerated))
                {
                    LogError("Job parameter " + clsAnalysisResourcesPRIDEConverter.GetGeneratedFastaParamNameForJob(intJob) +
                             " was not found in CreateMSGFReportFile; unable to continue");
                    return false;
                }

                clsDataPackageJobInfo dataPkgJob = null;

                if (!mDataPackagePeptideHitJobs.TryGetValue(intJob, out dataPkgJob))
                {
                    LogError("Job " + intJob + " not found in mDataPackagePeptideHitJobs; unable to continue");
                    return false;
                }

                if (!string.IsNullOrEmpty(dataPkgJob.ProteinCollectionList) && dataPkgJob.ProteinCollectionList != "na")
                {
                    strProteinCollectionListOrFasta = dataPkgJob.ProteinCollectionList;
                }
                else
                {
                    strProteinCollectionListOrFasta = dataPkgJob.LegacyFastaFileName;
                }

                if (mCachedOrgDBName != strOrgDBNameGenerated)
                {
                    // Need to read the proteins from the fasta file

                    mCachedProteins.Clear();
                    mCachedProteinPSMCounts.Clear();

                    string strFastaFilePath = Path.Combine(strLocalOrgDBFolder, strOrgDBNameGenerated);
                    var objFastaFileReader = new ProteinFileReader.FastaFileReader();

                    if (!objFastaFileReader.OpenFile(strFastaFilePath))
                    {
                        var msg = "Error opening fasta file " + strOrgDBNameGenerated + "; objFastaFileReader.OpenFile() returned false";
                        LogError(msg + "; see " + strLocalOrgDBFolder);
                        m_message = msg;
                        return false;
                    }
                    else
                    {
                        Console.WriteLine();
                        Console.WriteLine("Reading proteins from " + strFastaFilePath);

                        while (objFastaFileReader.ReadNextProteinEntry())
                        {
                            if (!mCachedProteins.ContainsKey(objFastaFileReader.ProteinName))
                            {
                                var kvIndexAndSequence = new KeyValuePair<int, string>(mCachedProteins.Count, objFastaFileReader.ProteinSequence);

                                try
                                {
                                    mCachedProteins.Add(objFastaFileReader.ProteinName, kvIndexAndSequence);
                                }
                                catch (Exception ex)
                                {
                                    throw new Exception("Dictionary error adding to mCachedProteins", ex);
                                }

                                try
                                {
                                    mCachedProteinPSMCounts.Add(kvIndexAndSequence.Key, 0);
                                }
                                catch (Exception ex)
                                {
                                    throw new Exception("Dictionary error adding to mCachedProteinPSMCounts", ex);
                                }
                            }
                        }
                        objFastaFileReader.CloseFile();
                    }

                    mCachedOrgDBName = string.Copy(strOrgDBNameGenerated);
                }
                else
                {
                    // Reset the counts in mCachedProteinPSMCounts
                    for (var intIndex = 0; intIndex <= mCachedProteinPSMCounts.Count; intIndex++)
                    {
                        mCachedProteinPSMCounts[intIndex] = 0;
                    }
                }

                lstPseudoMSGFData.Clear();

                strPseudoMsgfFilePath = CreatePseudoMSGFFileUsingPHRPReader(intJob, strDataset, udtFilterThresholds, lstPseudoMSGFData);

                if (string.IsNullOrEmpty(strPseudoMsgfFilePath))
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        LogError("Pseudo Msgf file not created for job " + intJob + ", dataset " + strDataset);
                    }
                    return false;
                }

                AddToListIfNew(mPreviousDatasetFilesToDelete, strPseudoMsgfFilePath);

                if (!mCreateMSGFReportFilesOnly)
                {
                    strPrideReportXMLFilePath = CreateMSGFReportXMLFile(strTemplateFileName, dataPkgJob, strPseudoMsgfFilePath, lstPseudoMSGFData,
                        strOrgDBNameGenerated, strProteinCollectionListOrFasta, udtFilterThresholds);

                    if (string.IsNullOrEmpty(strPrideReportXMLFilePath))
                    {
                        if (string.IsNullOrEmpty(m_message))
                        {
                            LogError("Pride report XML file not created for job " + intJob + ", dataset " + strDataset);
                        }
                        return false;
                    }
                }

                blnSuccess = true;
            }
            catch (Exception ex)
            {
                LogError("Exception in CreateMSGFReportFile for job " + intJob + ", dataset " + strDataset, ex);
                return false;
            }

            return blnSuccess;
        }

        private string CreateMSGFReportXMLFile(string strTemplateFileName, clsDataPackageJobInfo dataPkgJob, string strPseudoMsgfFilePath,
            IReadOnlyDictionary<string, List<udtPseudoMSGFDataType>> lstPseudoMSGFData, string strOrgDBNameGenerated,
            string strProteinCollectionListOrFasta, udtFilterThresholdsType udtFilterThresholds)
        {
            string strPrideReportXMLFilePath = null;

            bool blnInsideMzDataDescription = false;
            bool blnSkipNode = false;
            var blnInstrumentDetailsAutoDefined = false;

            var lstAttributeOverride = new Dictionary<string, string>();

            var eFileLocation = eMSGFReportXMLFileLocation.Header;
            var lstRecentElements = new Queue<string>();

            try
            {
                var lstElementCloseDepths = new Stack<int>();

                // Open strTemplateFileName and parse it to create a new XML file
                // Use a forward-only XML reader, copying some elements verbatim and customizing others
                // When we reach <Identifications>, we write out the data that was cached from strPseudoMsgfFilePath
                //    Must write out data by protein

                // Next, append the protein sequences in mCachedProteinPSMCounts to the <Fasta></Fasta> section

                // Finally, write the remaining sections
                // <PTMs>
                // <DatabaseMappings>
                // <ConfigurationOptions>

                strPrideReportXMLFilePath = strPseudoMsgfFilePath + "-report.xml";

                using (var objXmlWriter = new XmlTextWriter(new FileStream(strPrideReportXMLFilePath, FileMode.Create, FileAccess.Write, FileShare.Read), new UTF8Encoding(false)))
                using (var objXmlReader = new XmlTextReader(new FileStream(Path.Combine(m_WorkDir, strTemplateFileName), FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    objXmlWriter.Formatting = Formatting.Indented;
                    objXmlWriter.Indentation = 4;

                    objXmlWriter.WriteStartDocument();

                    while (objXmlReader.Read())
                    {
                        switch (objXmlReader.NodeType)
                        {
                            case XmlNodeType.Whitespace:
                                break;
                            // Skip whitespace since the writer should be auto-formatting things
                            // objXmlWriter.WriteWhitespace(objXmlReader.Value)

                            case XmlNodeType.Comment:
                                objXmlWriter.WriteComment(objXmlReader.Value);

                                break;
                            case XmlNodeType.Element:
                                // Start element

                                if (lstRecentElements.Count > 10)
                                    lstRecentElements.Dequeue();
                                lstRecentElements.Enqueue("Element " + objXmlReader.Name);

                                while (lstElementCloseDepths.Count > 0 && lstElementCloseDepths.Peek() > objXmlReader.Depth)
                                {
                                    lstElementCloseDepths.Pop();

                                    objXmlWriter.WriteEndElement();
                                }

                                eFileLocation = UpdateMSGFReportXMLFileLocation(eFileLocation, objXmlReader.Name, blnInsideMzDataDescription);

                                blnSkipNode = false;
                                lstAttributeOverride.Clear();

                                switch (objXmlReader.Name)
                                {
                                    case "sourceFilePath":
                                        // Update this element's value to contain strPseudoMsgfFilePath
                                        objXmlWriter.WriteElementString("sourceFilePath", strPseudoMsgfFilePath);
                                        blnSkipNode = true;

                                        break;
                                    case "timeCreated":
                                        // Write out the current date/time in this format: 2012-11-06T16:04:44Z
                                        objXmlWriter.WriteElementString("timeCreated", DateTime.Now.ToUniversalTime().ToString("s") + "Z");
                                        blnSkipNode = true;

                                        break;
                                    case "MzDataDescription":
                                        blnInsideMzDataDescription = true;

                                        break;
                                    case "sampleName":
                                        if (eFileLocation == eMSGFReportXMLFileLocation.MzDataAdmin)
                                        {
                                            // Write out the current job's Experiment Name
                                            objXmlWriter.WriteElementString("sampleName", dataPkgJob.Experiment);
                                            blnSkipNode = true;
                                        }

                                        break;
                                    case "sampleDescription":
                                        if (eFileLocation == eMSGFReportXMLFileLocation.MzDataAdmin)
                                        {
                                            // Override the comment attribute for this node
                                            string strCommentOverride = null;

                                            if (!string.IsNullOrWhiteSpace(dataPkgJob.Experiment_Reason))
                                            {
                                                strCommentOverride = dataPkgJob.Experiment_Reason.TrimEnd();

                                                if (!string.IsNullOrWhiteSpace(dataPkgJob.Experiment_Comment))
                                                {
                                                    if (strCommentOverride.EndsWith("."))
                                                    {
                                                        strCommentOverride += " " + dataPkgJob.Experiment_Comment;
                                                    }
                                                    else
                                                    {
                                                        strCommentOverride += ". " + dataPkgJob.Experiment_Comment;
                                                    }
                                                }
                                            }
                                            else
                                            {
                                                strCommentOverride = dataPkgJob.Experiment_Comment;
                                            }

                                            lstAttributeOverride.Add("comment", strCommentOverride);
                                        }

                                        break;
                                    case "sourceFile":
                                        if (eFileLocation == eMSGFReportXMLFileLocation.MzDataAdmin)
                                        {
                                            objXmlWriter.WriteStartElement("sourceFile");

                                            objXmlWriter.WriteElementString("nameOfFile", Path.GetFileName(strPseudoMsgfFilePath));
                                            objXmlWriter.WriteElementString("pathToFile", strPseudoMsgfFilePath);
                                            objXmlWriter.WriteElementString("fileType", "MSGF file");

                                            objXmlWriter.WriteEndElement();  // sourceFile
                                            blnSkipNode = true;
                                        }

                                        break;
                                    case "software":
                                        if (eFileLocation == eMSGFReportXMLFileLocation.MzDataDataProcessing)
                                        {
                                            CreateMSGFReportXmlFileWriteSoftwareVersion(objXmlReader, objXmlWriter, dataPkgJob.PeptideHitResultType);
                                            blnSkipNode = true;
                                        }

                                        break;
                                    case "instrumentName":
                                        if (eFileLocation == eMSGFReportXMLFileLocation.MzDataInstrument)
                                        {
                                            // Write out the actual instrument name
                                            objXmlWriter.WriteElementString("instrumentName", dataPkgJob.Instrument);
                                            blnSkipNode = true;

                                            blnInstrumentDetailsAutoDefined = WriteXMLInstrumentInfo(objXmlWriter, dataPkgJob.InstrumentGroup);
                                        }

                                        break;
                                    case "source":
                                    case "analyzerList":
                                    case "detector":
                                        if (eFileLocation == eMSGFReportXMLFileLocation.MzDataInstrument && blnInstrumentDetailsAutoDefined)
                                        {
                                            blnSkipNode = true;
                                        }

                                        break;
                                    case "cvParam":
                                        if (eFileLocation == eMSGFReportXMLFileLocation.ExperimentAdditional)
                                        {
                                            // Override the cvParam if it has Accession PRIDE:0000175

                                            objXmlWriter.WriteStartElement("cvParam");

                                            if (objXmlReader.HasAttributes)
                                            {
                                                string strValueOverride = string.Empty;
                                                objXmlReader.MoveToFirstAttribute();
                                                do
                                                {
                                                    if (objXmlReader.Name == "accession" && objXmlReader.Value == "PRIDE:0000175")
                                                    {
                                                        strValueOverride = "DMS PRIDE_Converter " + System.Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString();
                                                    }

                                                    if (objXmlReader.Name == "value" && strValueOverride.Length > 0)
                                                    {
                                                        objXmlWriter.WriteAttributeString(objXmlReader.Name, strValueOverride);
                                                    }
                                                    else
                                                    {
                                                        objXmlWriter.WriteAttributeString(objXmlReader.Name, objXmlReader.Value);
                                                    }
                                                } while (objXmlReader.MoveToNextAttribute());
                                            }

                                            objXmlWriter.WriteEndElement();  // cvParam
                                            blnSkipNode = true;
                                        }

                                        break;
                                    case "Identifications":
                                        if (!CreateMSGFReportXMLFileWriteIDs(objXmlWriter, lstPseudoMSGFData, strOrgDBNameGenerated))
                                        {
                                            LogError("CreateMSGFReportXMLFileWriteIDs returned false; aborting");
                                            return string.Empty;
                                        }

                                        if (!CreateMSGFReportXMLFileWriteProteins(objXmlWriter, strOrgDBNameGenerated))
                                        {
                                            LogError("CreateMSGFReportXMLFileWriteProteins returned false; aborting");
                                            return string.Empty;
                                        }

                                        blnSkipNode = true;

                                        break;
                                    case "Fasta":
                                        // This section is written out by CreateMSGFReportXMLFileWriteIDs
                                        blnSkipNode = true;

                                        break;
                                    case "PTMs":
                                        // In the future, we might write out customized PTMs in CreateMSGFReportXMLFileWriteProteins
                                        // For now, just copy over whatever is in the template msgf-report.xml file
                                        //
                                        blnSkipNode = false;

                                        break;
                                    case "DatabaseMappings":

                                        objXmlWriter.WriteStartElement("DatabaseMappings");
                                        objXmlWriter.WriteStartElement("DatabaseMapping");

                                        objXmlWriter.WriteElementString("SearchEngineDatabaseName", strOrgDBNameGenerated);
                                        objXmlWriter.WriteElementString("SearchEngineDatabaseVersion", "Unknown");

                                        objXmlWriter.WriteElementString("CuratedDatabaseName", strProteinCollectionListOrFasta);
                                        objXmlWriter.WriteElementString("CuratedDatabaseVersion", "1");

                                        objXmlWriter.WriteEndElement();      // DatabaseMapping
                                        objXmlWriter.WriteEndElement();      // DatabaseMappings

                                        blnSkipNode = true;

                                        break;
                                    case "ConfigurationOptions":
                                        objXmlWriter.WriteStartElement("ConfigurationOptions");

                                        WriteConfigurationOption(objXmlWriter, "search_engine", "MSGF");
                                        WriteConfigurationOption(objXmlWriter, "peptide_threshold", udtFilterThresholds.PValueThreshold.ToString("0.00"));
                                        WriteConfigurationOption(objXmlWriter, "add_carbamidomethylation", "false");

                                        objXmlWriter.WriteEndElement();      // ConfigurationOptions

                                        blnSkipNode = true;

                                        break;
                                }

                                if (blnSkipNode)
                                {
                                    if (objXmlReader.NodeType != XmlNodeType.EndElement)
                                    {
                                        // Skip this element (and any children nodes enclosed in this elemnt)
                                        // Likely should not do this when objXmlReader.NodeType is XmlNodeType.EndElement
                                        objXmlReader.Skip();
                                    }
                                }
                                else
                                {
                                    // Copy this element from the source file to the target file

                                    objXmlWriter.WriteStartElement(objXmlReader.Name);

                                    if (objXmlReader.HasAttributes)
                                    {
                                        objXmlReader.MoveToFirstAttribute();
                                        do
                                        {
                                            string strAttributeOverride = string.Empty;
                                            if (lstAttributeOverride.Count > 0 && lstAttributeOverride.TryGetValue(objXmlReader.Name, out strAttributeOverride))
                                            {
                                                objXmlWriter.WriteAttributeString(objXmlReader.Name, strAttributeOverride);
                                            }
                                            else
                                            {
                                                objXmlWriter.WriteAttributeString(objXmlReader.Name, objXmlReader.Value);
                                            }
                                        } while (objXmlReader.MoveToNextAttribute());

                                        lstElementCloseDepths.Push(objXmlReader.Depth);
                                    }
                                    else if (objXmlReader.IsEmptyElement)
                                    {
                                        objXmlWriter.WriteEndElement();
                                    }
                                }

                                break;
                            case XmlNodeType.EndElement:

                                if (lstRecentElements.Count > 10)
                                    lstRecentElements.Dequeue();
                                lstRecentElements.Enqueue("EndElement " + objXmlReader.Name);

                                while (lstElementCloseDepths.Count > 0 && lstElementCloseDepths.Peek() > objXmlReader.Depth + 1)
                                {
                                    lstElementCloseDepths.Pop();
                                    objXmlWriter.WriteEndElement();
                                }

                                objXmlWriter.WriteEndElement();

                                if (objXmlReader.Name == "MzDataDescription")
                                {
                                    blnInsideMzDataDescription = false;
                                }

                                while (lstElementCloseDepths.Count > 0 && lstElementCloseDepths.Peek() > objXmlReader.Depth)
                                {
                                    lstElementCloseDepths.Pop();
                                }

                                break;
                            case XmlNodeType.Text:

                                if (!string.IsNullOrEmpty(objXmlReader.Value))
                                {
                                    if (lstRecentElements.Count > 10)
                                        lstRecentElements.Dequeue();
                                    if (objXmlReader.Value.Length > 10)
                                    {
                                        lstRecentElements.Enqueue(objXmlReader.Value.Substring(0, 10));
                                    }
                                    else
                                    {
                                        lstRecentElements.Enqueue(objXmlReader.Value);
                                    }
                                }

                                objXmlWriter.WriteString(objXmlReader.Value);

                                break;
                        }
                    }

                    objXmlWriter.WriteEndDocument();
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in CreateMSGFReportXMLFile", ex);

                string strRecentElements = string.Empty;
                foreach (var strItem in lstRecentElements)
                {
                    if (string.IsNullOrEmpty(strRecentElements))
                    {
                        strRecentElements = string.Copy(strItem);
                    }
                    else
                    {
                        strRecentElements += "; " + strItem;
                    }
                }

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strRecentElements);

                return string.Empty;
            }

            return strPrideReportXMLFilePath;
        }

        private bool CreateMSGFReportXMLFileWriteIDs(XmlTextWriter objXmlWriter,
            IReadOnlyDictionary<string, List<udtPseudoMSGFDataType>> lstPseudoMSGFData, string strOrgDBNameGenerated)
        {
            try
            {
                objXmlWriter.WriteStartElement("Identifications");

                foreach (KeyValuePair<string, List<udtPseudoMSGFDataType>> kvProteinEntry in lstPseudoMSGFData)
                {
                    KeyValuePair<int, string> kvIndexAndSequence;

                    if (!mCachedProteins.TryGetValue(kvProteinEntry.Key, out kvIndexAndSequence))
                    {
                        // Protein not found in mCachedProteins; this is unexpected (should have already been added by CreatePseudoMSGFFileUsingPHRPReader()
                        // Add the protein to mCachedProteins and mCachedProteinPSMCounts, though we won't know its sequence

                        kvIndexAndSequence = new KeyValuePair<int, string>(mCachedProteins.Count, string.Empty);
                        mCachedProteinPSMCounts.Add(kvIndexAndSequence.Key, kvProteinEntry.Value.Count);
                        mCachedProteins.Add(kvProteinEntry.Key, kvIndexAndSequence);
                    }
                    else
                    {
                        mCachedProteinPSMCounts[kvIndexAndSequence.Key] = kvProteinEntry.Value.Count;
                    }

                    objXmlWriter.WriteStartElement("Identification");

                    objXmlWriter.WriteElementString("Accession", kvProteinEntry.Key);            // Protein name
                    // objXmlWriter.WriteElementString("CuratedAccession", kvProteinEntry.Key);     // Cleaned-up version of the Protein name; for example, for ref|NP_035862.2 we would put "NP_035862" here
                    objXmlWriter.WriteElementString("UniqueIdentifier", kvProteinEntry.Key);     // Protein name
                    // objXmlWriter.WriteElementString("AccessionVersion", "1");                    // Accession version would be determined when curating the "Accession" name.  For example, for ref|NP_035862.2 we would put "2" here
                    objXmlWriter.WriteElementString("Database", strOrgDBNameGenerated);
                    objXmlWriter.WriteElementString("DatabaseVersion", "Unknown");

                    // Write out each PSM for this protein
                    foreach (udtPseudoMSGFDataType udtPeptide in kvProteinEntry.Value)
                    {
                        objXmlWriter.WriteStartElement("Peptide");

                        objXmlWriter.WriteElementString("Sequence", udtPeptide.CleanSequence);
                        objXmlWriter.WriteElementString("CuratedSequence", string.Empty);
                        objXmlWriter.WriteElementString("Start", "0");
                        objXmlWriter.WriteElementString("End", "0");
                        objXmlWriter.WriteElementString("SpectrumReference", udtPeptide.ScanNumber.ToString());

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

                        objXmlWriter.WriteElementString("isSpecific", "false");

                        objXmlWriter.WriteElementString("UniqueIdentifier", udtPeptide.ScanNumber.ToString());       // I wanted to record ResultID here, but we instead have to record Scan Number; otherwise PRIDE Converter Crashes

                        objXmlWriter.WriteStartElement("additional");

                        WriteCVParam(objXmlWriter, "PRIDE", "PRIDE:0000065", "Upstream flanking sequence", udtPeptide.PrefixResidue);
                        WriteCVParam(objXmlWriter, "PRIDE", "PRIDE:0000066", "Downstream flanking sequence", udtPeptide.SuffixResidue);

                        WriteCVParam(objXmlWriter, "MS", "MS:1000041", "charge state", udtPeptide.ChargeState.ToString());
                        WriteCVParam(objXmlWriter, "MS", "MS:1000042", "peak intensity", "0.0");
                        WriteCVParam(objXmlWriter, "MS", "MS:1001870", "p-value for peptides", udtPeptide.PValue);

                        WriteUserParam(objXmlWriter, "MQScore", udtPeptide.MQScore);
                        WriteUserParam(objXmlWriter, "TotalPRMScore", udtPeptide.TotalPRMScore);

                        // WriteUserParam(objXmlWriter, "MedianPRMScore", "0.0")
                        // WriteUserParam(objXmlWriter, "FractionY", "0.0")
                        // WriteUserParam(objXmlWriter, "FractionB", "0.0")

                        WriteUserParam(objXmlWriter, "NTT", udtPeptide.NTT.ToString());

                        // WriteUserParam(objXmlWriter, "F-Score", "0.0")

                        WriteUserParam(objXmlWriter, "DeltaScore", udtPeptide.DeltaScore);
                        WriteUserParam(objXmlWriter, "DeltaScoreOther", udtPeptide.DeltaScoreOther);
                        WriteUserParam(objXmlWriter, "SpecProb", udtPeptide.MSGFSpecProb);

                        objXmlWriter.WriteEndElement();      // additional

                        objXmlWriter.WriteEndElement();      // Peptide
                    }

                    // Protein level-scores
                    objXmlWriter.WriteElementString("Score", "0.0");
                    objXmlWriter.WriteElementString("Threshold", "0.0");
                    objXmlWriter.WriteElementString("SearchEngine", "MSGF");

                    objXmlWriter.WriteStartElement("additional");
                    objXmlWriter.WriteEndElement();

                    objXmlWriter.WriteElementString("FastaSequenceReference", kvIndexAndSequence.Key.ToString());

                    objXmlWriter.WriteEndElement();      // Identification
                }

                objXmlWriter.WriteEndElement();          // Identifications
            }
            catch (Exception ex)
            {
                LogError("Exception in CreateMSGFReportXMLFileWriteIDs", ex);
                return false;
            }

            return true;
        }

        private bool CreateMSGFReportXMLFileWriteProteins(XmlTextWriter objXmlWriter, string strOrgDBNameGenerated)
        {
            string strProteinName = null;
            int intProteinIndex = 0;
            int intPSMCount = 0;

            try
            {
                objXmlWriter.WriteStartElement("Fasta");
                objXmlWriter.WriteAttributeString("sourceDb", strOrgDBNameGenerated);
                objXmlWriter.WriteAttributeString("sourceDbVersion", "Unknown");

                // Step through mCachedProteins
                // For each entry, the key is the protein name
                // The value is itself a key-value pair, where Value.Key is the protein index and Value.Value is the protein sequence

                foreach (KeyValuePair<string, KeyValuePair<int, string>> kvEntry in mCachedProteins)
                {
                    strProteinName = string.Copy(kvEntry.Key);
                    intProteinIndex = kvEntry.Value.Key;

                    // Only write out this protein if it had 1 or more PSMs
                    if (mCachedProteinPSMCounts.TryGetValue(intProteinIndex, out intPSMCount))
                    {
                        if (intPSMCount > 0)
                        {
                            objXmlWriter.WriteStartElement("Sequence");
                            objXmlWriter.WriteAttributeString("id", intProteinIndex.ToString());
                            objXmlWriter.WriteAttributeString("accession", strProteinName);

                            objXmlWriter.WriteValue(kvEntry.Value.Value);

                            objXmlWriter.WriteEndElement();          // Sequence
                        }
                    }
                }

                objXmlWriter.WriteEndElement();          // Fasta

                // In the future, we might write out customized PTMs here
                // For now, just copy over whatever is in the template msgf-report.xml file
                //
                //objXmlWriter.WriteStartElement("PTMs")
                //objXmlWriter.WriteFullEndElement()
            }
            catch (Exception ex)
            {
                LogError("Exception in CreateMSGFReportXMLFileWriteProteins", ex);
                return false;
            }

            return true;
        }

        private void CreateMSGFReportXmlFileWriteSoftwareVersion(XmlTextReader objXmlReader, XmlTextWriter objXmlWriter,
            clsPHRPReader.ePeptideHitResultType PeptideHitResultType)
        {
            string strToolName = string.Empty;
            string strToolVersion = string.Empty;
            string strToolComments = string.Empty;
            int intNodeDepth = objXmlReader.Depth;

            // Read the name, version, and comments elements under software
            while (objXmlReader.Read())
            {
                var error = false;
                switch (objXmlReader.NodeType)
                {
                    case XmlNodeType.Element:
                        switch (objXmlReader.Name)
                        {
                            case "name":
                                strToolName = objXmlReader.ReadElementContentAsString();
                                break;
                            case "version":
                                strToolVersion = objXmlReader.ReadElementContentAsString();
                                break;
                            case "comments":
                                strToolComments = objXmlReader.ReadElementContentAsString();
                                break;
                        }
                        break;
                    case XmlNodeType.EndElement:
                        if (objXmlReader.Depth <= intNodeDepth)
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

            if (string.IsNullOrEmpty(strToolName))
            {
                strToolName = PeptideHitResultType.ToString();
                strToolVersion = string.Empty;
                strToolComments = string.Empty;
            }
            else
            {
                if (PeptideHitResultType == clsPHRPReader.ePeptideHitResultType.MSGFDB && strToolName.ToUpper().StartsWith("MSGF"))
                {
                    // Tool Version in the template file is likely correct; use it
                }
                else if (PeptideHitResultType == clsPHRPReader.ePeptideHitResultType.Sequest && strToolName.ToUpper().StartsWith("SEQUEST"))
                {
                    // Tool Version in the template file is likely correct; use it
                }
                else if (PeptideHitResultType == clsPHRPReader.ePeptideHitResultType.XTandem && strToolName.ToUpper().Contains("TANDEM"))
                {
                    // Tool Version in the template file is likely correct; use it
                }
                else
                {
                    // Tool Version is likely not known
                    strToolName = PeptideHitResultType.ToString();
                    strToolVersion = string.Empty;
                    strToolComments = string.Empty;
                }
            }

            objXmlWriter.WriteStartElement("software");

            objXmlWriter.WriteElementString("name", strToolName);
            objXmlWriter.WriteElementString("version", strToolVersion);
            objXmlWriter.WriteElementString("comments", strToolComments);

            objXmlWriter.WriteEndElement();  // software
        }

        /// <summary>
        /// Create the .msgf-pride.xml file using the .msgf-report.xml file
        /// </summary>
        /// <param name="intJob"></param>
        /// <param name="strDataset"></param>
        /// <param name="strPrideReportXMLFilePath"></param>
        /// <param name="strPrideXmlFilePath">Output parameter: the full path of the newly created .msgf-pride.xml file</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        private bool CreatePrideXMLFile(int intJob, string strDataset, string strPrideReportXMLFilePath, out string strPrideXmlFilePath)
        {
            bool blnSuccess = false;
            string strCurrentTask = null;

            string strBaseFileName = null;
            string strMsgfResultsFilePath = null;
            string strMzXMLFilePath = null;

            strPrideXmlFilePath = string.Empty;

            try
            {
                strBaseFileName = Path.GetFileName(strPrideReportXMLFilePath).Replace(FILE_EXTENSION_MSGF_REPORT_XML, string.Empty);
                strMsgfResultsFilePath = Path.Combine(m_WorkDir, strBaseFileName + FILE_EXTENSION_PSEUDO_MSGF);
                strMzXMLFilePath = Path.Combine(m_WorkDir, strDataset + clsAnalysisResources.DOT_MZXML_EXTENSION);
                strPrideReportXMLFilePath = Path.Combine(m_WorkDir, strBaseFileName + FILE_EXTENSION_MSGF_REPORT_XML);

                strCurrentTask = "Running PRIDE Converter for job " + intJob + ", " + strDataset;
                if (m_DebugLevel >= 1)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strCurrentTask);
                }

                blnSuccess = RunPrideConverter(intJob, strDataset, strMsgfResultsFilePath, strMzXMLFilePath, strPrideReportXMLFilePath);

                if (!blnSuccess)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        LogError("Unknown error calling RunPrideConverter", m_message);
                    }
                }
                else
                {
                    // Make sure the result file was created
                    strPrideXmlFilePath = Path.Combine(m_WorkDir, strBaseFileName + FILE_EXTENSION_MSGF_PRIDE_XML);
                    if (!File.Exists(strPrideXmlFilePath))
                    {
                        LogError("Pride XML file not created for job " + intJob + ": " + strPrideXmlFilePath);
                        return false;
                    }
                }

                blnSuccess = true;
            }
            catch (Exception ex)
            {
                LogError("Exception in CreatePrideXMLFile for job " + intJob + ", dataset " + strDataset, ex);
                return false;
            }

            return blnSuccess;
        }

        private bool CreatePXSubmissionFile(IReadOnlyDictionary<string, string> dctTemplateParameters)
        {
            const string TBD = "******* UPDATE ****** ";

            int intPrideXmlFilesCreated = 0;
            int intRawFilesStored = 0;
            int intPeakFilesStored = 0;
            int intMzIDFilesStored = 0;

            string strSubmissionType = null;
            string strFilterText = string.Empty;

            string strPXFilePath = null;

            try
            {
                strPXFilePath = Path.Combine(m_WorkDir, "PX_Submission_" + DateTime.Now.ToString("yyyy-MM-dd_HH-mm") + ".px");

                intPrideXmlFilesCreated = CountResultFilesByType(clsPXFileInfoBase.ePXFileType.Result);
                intRawFilesStored = CountResultFilesByType(clsPXFileInfoBase.ePXFileType.Raw);
                intPeakFilesStored = CountResultFilesByType(clsPXFileInfoBase.ePXFileType.Peak);
                intMzIDFilesStored = CountResultFilesByType(clsPXFileInfoBase.ePXFileType.ResultMzId);

                if (m_DebugLevel >= 1)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Creating PXSubmission file: " + strPXFilePath);
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        " Result stats: " + intPrideXmlFilesCreated + " Result (.msgf-pride.xml) files");
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        " Result stats: " + intRawFilesStored + " Raw files");
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        " Result stats: " + intPeakFilesStored + " Peak (.mgf) files");
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        " Result stats: " + intMzIDFilesStored + " Search (.mzid.gz) files");
                }

                if (intMzIDFilesStored == 0 && intPrideXmlFilesCreated == 0)
                {
                    strSubmissionType = PARTIAL_SUBMISSION;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
                        "Did not create any Pride XML result files; submission type is " + strSubmissionType);
                }
                else if (intPrideXmlFilesCreated > 0 && intMzIDFilesStored > intPrideXmlFilesCreated)
                {
                    strSubmissionType = PARTIAL_SUBMISSION;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
                        "Stored more Search (.mzid.gz) files than Pride XML result files; submission type is " + strSubmissionType);
                }
                else if (intPrideXmlFilesCreated > 0 && intRawFilesStored > intPrideXmlFilesCreated)
                {
                    strSubmissionType = PARTIAL_SUBMISSION;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
                        "Stored more Raw files than Pride XML result files; submission type is " + strSubmissionType);
                }
                else if (intMzIDFilesStored == 0)
                {
                    strSubmissionType = PARTIAL_SUBMISSION;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
                        "Did not have any .mzid.gz files and did not create any Pride XML result files; submission type is " + strSubmissionType);
                }
                else
                {
                    strSubmissionType = COMPLETE_SUBMISSION;

                    if (mFilterThresholdsUsed.UseFDRThreshold || mFilterThresholdsUsed.UsePepFDRThreshold || mFilterThresholdsUsed.UseMSGFSpecProb)
                    {
                        const string strFilterTextBase = "msgf-pride.xml files are filtered on ";
                        strFilterText = string.Empty;

                        if (mFilterThresholdsUsed.UseFDRThreshold)
                        {
                            if (string.IsNullOrEmpty(strFilterText))
                            {
                                strFilterText = strFilterTextBase;
                            }
                            else
                            {
                                strFilterText += " and ";
                            }

                            strFilterText += (mFilterThresholdsUsed.FDRThreshold * 100).ToString("0.0") + "% FDR at the PSM level";
                        }

                        if (mFilterThresholdsUsed.UsePepFDRThreshold)
                        {
                            if (string.IsNullOrEmpty(strFilterText))
                            {
                                strFilterText = strFilterTextBase;
                            }
                            else
                            {
                                strFilterText += " and ";
                            }

                            strFilterText += (mFilterThresholdsUsed.PepFDRThreshold * 100).ToString("0.0") + "% FDR at the peptide level";
                        }

                        if (mFilterThresholdsUsed.UseMSGFSpecProb)
                        {
                            if (string.IsNullOrEmpty(strFilterText))
                            {
                                strFilterText = strFilterTextBase;
                            }
                            else
                            {
                                strFilterText += " and ";
                            }

                            strFilterText += "MSGF Spectral Probability <= " + mFilterThresholdsUsed.MSGFSpecProbThreshold.ToString("0.0E+00");
                        }
                    }
                }

                using (var swPXFile = new StreamWriter(new FileStream(strPXFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    WritePXHeader(swPXFile, "submitter_name", "Matthew Monroe", dctTemplateParameters);
                    WritePXHeader(swPXFile, "submitter_email", "matthew.monroe@pnnl.gov", dctTemplateParameters);
                    WritePXHeader(swPXFile, "submitter_affiliation", PNNL_NAME_COUNTRY, dctTemplateParameters);
                    WritePXHeader(swPXFile, "submitter_pride_login", "matthew.monroe@pnl.gov", dctTemplateParameters);

                    WritePXHeader(swPXFile, "lab_head_name", "Richard D. Smith", dctTemplateParameters);
                    WritePXHeader(swPXFile, "lab_head_email", "dick.smith@pnnl.gov", dctTemplateParameters);
                    WritePXHeader(swPXFile, "lab_head_affiliation", PNNL_NAME_COUNTRY, dctTemplateParameters);

                    WritePXHeader(swPXFile, "project_title", TBD + "User-friendly Article Title", dctTemplateParameters);
                    WritePXHeader(swPXFile, "project_description", TBD + "Summary sentence", dctTemplateParameters, 50);         // Minimum 50 characterse, max 5000 characters

                    // We don't normally use the project_tag field, so it is commented out
                    // Example official tags are:
                    //  Human proteome project
                    //  Human plasma project
                    //WritePXHeader(swPXFile, "project_tag", TBD & "Official project tag assigned by the repository", dctTemplateParameters)

                    if (dctTemplateParameters.ContainsKey("pubmed_id"))
                    {
                        WritePXHeader(swPXFile, "pubmed_id", TBD, dctTemplateParameters);
                    }

                    // We don't normally use this field, so it is commented out
                    // WritePXHeader(swPXFile, "other_omics_link", "Related data is available from PeptideAtlas at http://www.peptideatlas.org/PASS/PASS00297")

                    WritePXHeader(swPXFile, "keywords", TBD, dctTemplateParameters);                                 // Comma separated list; suggest at least 3 keywords
                    WritePXHeader(swPXFile, "sample_processing_protocol", TBD, dctTemplateParameters, 50);           // Minimum 50 characters, max 5000 characters
                    WritePXHeader(swPXFile, "data_processing_protocol", TBD, dctTemplateParameters, 50);             // Minimum 50 characters, max 5000 characters

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
                    WritePXHeader(swPXFile, "experiment_type", GetCVString("PRIDE", "PRIDE:0000429", "Shotgun proteomics", ""), dctTemplateParameters);

                    WritePXLine(swPXFile, new List<string>
                    {
                        "MTD",
                        "submission_type",
                        strSubmissionType
                    });

                    if (strSubmissionType == COMPLETE_SUBMISSION)
                    {
                        // Note that the comment field has been deprecated in v2.x of the px file
                        // However, we don't have a good alternative place to put this comment, so we'll include it anyway
                        if (!string.IsNullOrWhiteSpace(strFilterText))
                        {
                            WritePXHeader(swPXFile, "comment", strFilterText);
                        }
                    }
                    else
                    {
                        var strComment = "Data produced by the DMS Processing pipeline using ";
                        if (mSearchToolsUsed.Count == 1)
                        {
                            strComment += "search tool " + mSearchToolsUsed.First();
                        }
                        else if (mSearchToolsUsed.Count == 2)
                        {
                            strComment += "search tools " + mSearchToolsUsed.First() + " and " + mSearchToolsUsed.Last();
                        }
                        else if (mSearchToolsUsed.Count > 2)
                        {
                            strComment += "search tools " + string.Join(", ", (from item in mSearchToolsUsed where item != mSearchToolsUsed.Last() orderby item select item).ToList());
                            strComment += ", and " + mSearchToolsUsed.Last();
                        }

                        WritePXHeader(swPXFile, "reason_for_partial", strComment);
                    }

                    if (mExperimentNEWTInfo.Count == 0)
                    {
                        // None of the data package jobs had valid NEWT info
                        WritePXHeader(swPXFile, "species", TBD + GetCVString("NEWT", "2323", "unclassified Bacteria", ""), dctTemplateParameters);
                    }
                    else
                    {
                        // NEWT info is defined; write it out
                        foreach (var item in mExperimentNEWTInfo)
                        {
                            WritePXHeader(swPXFile, "species", GetNEWTCv(item.Key, item.Value));
                        }
                    }

                    WritePXHeader(swPXFile, "tissue", TBD + DEFAULT_TISSUE_CV, dctTemplateParameters);
                    WritePXHeader(swPXFile, "cell_type", TBD + "Optional, e.g. " + DEFAULT_CELL_TYPE_CV + DELETION_WARNING, dctTemplateParameters);
                    WritePXHeader(swPXFile, "disease", TBD + "Optional, e.g. " + DEFAULT_DISEASE_TYPE_CV + DELETION_WARNING, dctTemplateParameters);

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
                    WritePXHeader(swPXFile, "quantification", TBD + "Optional, e.g. " + DEFAULT_QUANTIFICATION_TYPE_CV, dctTemplateParameters);

                    if (mInstrumentGroupsStored.Count > 0)
                    {
                        WritePXInstruments(swPXFile);
                    }
                    else
                    {
                        // Instrument type is unknown
                        WritePXHeader(swPXFile, "instrument", TBD + GetCVString("MS", "MS:1000031", "instrument model", "CUSTOM UNKNOWN MASS SPEC"), dctTemplateParameters);
                    }

                    // Note that the modification terms are optional for complete submissions
                    // However, it doesn't hurt to include them
                    WritePXMods(swPXFile);

                    // Could write additional terms here
                    // WritePXHeader(swPXFile, "additional", GetCVString("", "", "Patient", "Colorectal cancer patient 1"), dctTemplateParameters)

                    // If this is a re-submission or re-analysis, then use these:
                    // WritePXHeader(swPXFile, "resubmission_px", "PXD00001", dctTemplateParameters)
                    // WritePXHeader(swPXFile, "reanalysis_px", "PXD00001", dctTemplateParameters)

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

                    var lstFileInfoCols = new List<string>();

                    // Keys in this dictionary are fileIDs, values are file names
                    var lstResultFileIDs = new Dictionary<int, string>();

                    // Append the files and mapping information to the ProteomeXchange PX file
                    foreach (var item in mPxResultFiles)
                    {
                        lstFileInfoCols.Clear();

                        lstFileInfoCols.Add("FME");
                        lstFileInfoCols.Add(item.Key.ToString());                    // file_id
                        var fileTypeName = PXFileTypeName(item.Value.PXFileType);
                        lstFileInfoCols.Add(fileTypeName);                           // file_type; allowed values are result, raw, peak, search, quantification, gel, other
                        lstFileInfoCols.Add(Path.Combine("D:\\Upload", m_ResFolderName, item.Value.Filename));    // file_path

                        var lstFileMappings = new List<string>();
                        foreach (var mapID in item.Value.FileMappings)
                        {
                            lstFileMappings.Add(mapID.ToString());                   // file_mapping
                        }

                        lstFileInfoCols.Add(string.Join(",", lstFileMappings));

                        WritePXLine(swPXFile, lstFileInfoCols);

                        if (fileTypeName == "RESULT")
                        {
                            lstResultFileIDs.Add(item.Key, item.Value.Filename);
                        }
                    }

                    // Determine whether the tissue or cell_type columns will bein the SMH section
                    bool smhIncludesCellType = DictionaryHasDefinedValue(dctTemplateParameters, "cell_type");
                    bool smhIncludesDisease = DictionaryHasDefinedValue(dctTemplateParameters, "disease");

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
                    foreach (var resultFile in lstResultFileIDs)
                    {
                        lstFileInfoCols.Clear();

                        lstFileInfoCols.Add("SME");
                        lstFileInfoCols.Add(resultFile.Key.ToString());          // file_id

                        var sampleMetadata = new clsSampleMetadata();
                        var resultFileName = resultFile.Value;
                        if (!mMzIdSampleInfo.TryGetValue(resultFile.Value, out sampleMetadata))
                        {
                            // Result file name may have been customized to include _Job1000000
                            // Check for this, and update resultFileName if required

                            var reMatch = reJobAddon.Match(resultFileName);
                            if (reMatch.Success)
                            {
                                var resultFileNameNew = resultFileName.Substring(0, reMatch.Index) + reMatch.Groups[2].Value.ToString() +
                                                        resultFileName.Substring(reMatch.Index + reMatch.Length);
                                resultFileName = resultFileNameNew;
                            }
                        }

                        if (mMzIdSampleInfo.TryGetValue(resultFileName, out sampleMetadata))
                        {
                            lstFileInfoCols.Add(sampleMetadata.Species);      // species
                            lstFileInfoCols.Add(sampleMetadata.Tissue);       // tissue

                            if (smhIncludesCellType)
                            {
                                lstFileInfoCols.Add(sampleMetadata.CellType);     // cell_type
                            }
                            else
                            {
                                lstFileInfoCols.Add(string.Empty);
                            }

                            if (smhIncludesDisease)
                            {
                                lstFileInfoCols.Add(sampleMetadata.Disease);      // disease
                            }
                            else
                            {
                                lstFileInfoCols.Add(string.Empty);
                            }

                            string strMods = string.Empty;
                            foreach (var modEntry in sampleMetadata.Modifications)
                            {
                                if (strMods.Length > 0)
                                    strMods += ", ";
                                strMods += GetCVString(modEntry.Value);
                            }
                            lstFileInfoCols.Add(strMods);                            // modification

                            var instrumentAccession = string.Empty;
                            var instrumentDescription = string.Empty;
                            GetInstrumentAccession(sampleMetadata.InstrumentGroup, out instrumentAccession, out instrumentDescription);

                            var strInstrumentCV = GetInstrumentCv(instrumentAccession, instrumentDescription);
                            lstFileInfoCols.Add(strInstrumentCV);                            // instrument

                            lstFileInfoCols.Add(GetValueOrDefault("quantification)", dctTemplateParameters, sampleMetadata.Quantification));           // quantification
                            lstFileInfoCols.Add(sampleMetadata.ExperimentalFactor);               // experimental_factor
                        }
                        else
                        {
                            LogWarning(" Sample Metadata not found for " + resultFile.Value);
                        }

                        WritePXLine(swPXFile, lstFileInfoCols);
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
            // JavaProgLoc will typically be "C:\Program Files\Java\jre7\bin\Java.exe"
            var JavaProgLoc = GetJavaProgLoc();
            if (string.IsNullOrEmpty(JavaProgLoc))
            {
                return false;
            }

            // Determine the path to the PRIDEConverter program
            mPrideConverterProgLoc = DetermineProgramLocation("PRIDEConverter", "PRIDEConverterProgLoc", "pride-converter-2.0-SNAPSHOT.jar");

            if (string.IsNullOrEmpty(mPrideConverterProgLoc))
            {
                if (string.IsNullOrEmpty(m_message))
                {
                    LogError("Error determining PrideConverter program location");
                }
                return false;
            }

            mMSXmlGeneratorAppPath = base.GetMSXmlGeneratorAppPath();

            return true;
        }

        private bool DefinePxFileMapping(int intFileID, int intParentFileID)
        {
            clsPXFileInfo oPXFileInfo = null;

            if (!mPxResultFiles.TryGetValue(intFileID, out oPXFileInfo))
            {
                LogError("FileID " + intFileID + " not found in mPxResultFiles; unable to add parent file");
                return false;
            }

            oPXFileInfo.AddFileMapping(intParentFileID);

            return true;
        }

        private bool DictionaryHasDefinedValue(IReadOnlyDictionary<string, string> dctTemplateParameters, string termName)
        {
            string value = string.Empty;

            if (dctTemplateParameters.TryGetValue(termName, out value))
            {
                if (!string.IsNullOrWhiteSpace(value))
                    return true;
            }

            return false;
        }

        private string GetCVString(clsSampleMetadata.udtCvParamInfoType cvParamInfo)
        {
            return GetCVString(cvParamInfo.CvRef, cvParamInfo.Accession, cvParamInfo.Name, cvParamInfo.Value);
        }

        private string GetCVString(string cvRef, string accession, string name, string value)
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
            string strInstrumentCV = null;

            if (string.IsNullOrEmpty(accession))
            {
                strInstrumentCV = GetCVString("MS", "MS:1000031", "instrument model", "CUSTOM UNKNOWN MASS SPEC");
            }
            else
            {
                strInstrumentCV = GetCVString("MS", accession, description, "");
            }

            return strInstrumentCV;
        }

        private string GetNEWTCv(int newtID, string newtName)
        {
            if (newtID == 0 & string.IsNullOrWhiteSpace(newtName))
            {
                newtID = 2323;
                newtName = "unclassified Bacteria";
            }

            return GetCVString("NEWT", newtID.ToString(), newtName, "");
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

        private string GetPrideConverterVersion(string strPrideConverterProgLoc)
        {
            string CmdStr = null;
            string strVersionFilePath = null;
            var strPRIDEConverterVersion = "unknown";

            mCmdRunner = new clsRunDosProgram(m_WorkDir);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            m_StatusTools.CurrentOperation = "Determining PrideConverter Version";
            m_StatusTools.UpdateAndWrite(m_progress);
            strVersionFilePath = Path.Combine(m_WorkDir, "PRIDEConverter_Version.txt");

            CmdStr = "-jar " + PossiblyQuotePath(strPrideConverterProgLoc);

            CmdStr += " -converter -version";

            if (m_DebugLevel >= 2)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, mJavaProgLoc + " " + CmdStr);
            }

            mCmdRunner.CreateNoWindow = false;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = false;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = strVersionFilePath;
            mCmdRunner.WorkDir = m_WorkDir;

            bool blnSuccess = false;
            blnSuccess = mCmdRunner.RunProgram(mJavaProgLoc, CmdStr, "PrideConverter", true);

            // Assure that the console output file has been parsed
            ParseConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath);

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (!blnSuccess)
            {
                LogError("Error running PrideConverter to determine its version");
            }
            else
            {
                var fiVersionFile = new FileInfo(strVersionFilePath);

                if (fiVersionFile.Exists)
                {
                    // Open the version file and read the version
                    using (var srVersionFile = new StreamReader(new FileStream(fiVersionFile.FullName, FileMode.Open, FileAccess.Read)))
                    {
                        if (!srVersionFile.EndOfStream)
                        {
                            strPRIDEConverterVersion = srVersionFile.ReadLine();
                        }
                    }
                }
            }

            return strPRIDEConverterVersion;
        }

        private string GetValueOrDefault(string strType, IReadOnlyDictionary<string, string> dctParameters, string defaultValue)
        {
            string strValueOverride = string.Empty;

            if (dctParameters.TryGetValue(strType, out strValueOverride))
            {
                return strValueOverride;
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
            mPxMasterFileList = new Dictionary<string, clsPXFileInfoBase>(StringComparer.CurrentCultureIgnoreCase);
            mPxResultFiles = new Dictionary<int, clsPXFileInfo>();

            // Initialize the CDTAFileStats dictionary
            mCDTAFileStats = new Dictionary<string, clsPXFileInfoBase>(StringComparer.CurrentCultureIgnoreCase);

            // Clear the previous dataset objects
            mPreviousDatasetName = string.Empty;
            mPreviousDatasetFilesToDelete = new List<string>();
            mPreviousDatasetFilesToCopy = new List<string>();

            // Initialize additional items
            mFilterThresholdsUsed = new udtFilterThresholdsType();
            mInstrumentGroupsStored = new Dictionary<string, List<string>>();
            mSearchToolsUsed = new SortedSet<string>();
            mExperimentNEWTInfo = new Dictionary<int, string>();

            mModificationsUsed = new Dictionary<string, clsSampleMetadata.udtCvParamInfoType>(StringComparer.CurrentCultureIgnoreCase);

            mMzIdSampleInfo = new Dictionary<string, clsSampleMetadata>(StringComparer.CurrentCultureIgnoreCase);

            // Determine the filter thresholds
            udtFilterThresholdsType udtFilterThresholds = new udtFilterThresholdsType();
            udtFilterThresholds.Clear();
            udtFilterThresholds.PValueThreshold = m_jobParams.GetJobParameter("PValueThreshold", udtFilterThresholds.PValueThreshold);
            udtFilterThresholds.FDRThreshold = m_jobParams.GetJobParameter("FDRThreshold", udtFilterThresholds.FDRThreshold);
            udtFilterThresholds.PepFDRThreshold = m_jobParams.GetJobParameter("PepFDRThreshold", udtFilterThresholds.PepFDRThreshold);
            udtFilterThresholds.MSGFSpecProbThreshold = m_jobParams.GetJobParameter("MSGFSpecProbThreshold", udtFilterThresholds.MSGFSpecProbThreshold);

            udtFilterThresholds.UseFDRThreshold = m_jobParams.GetJobParameter("UseFDRThreshold", udtFilterThresholds.UseFDRThreshold);
            udtFilterThresholds.UsePepFDRThreshold = m_jobParams.GetJobParameter("UsePepFDRThreshold", udtFilterThresholds.UsePepFDRThreshold);
            udtFilterThresholds.UseMSGFSpecProb = m_jobParams.GetJobParameter("UseMSGFSpecProb", udtFilterThresholds.UseMSGFSpecProb);

            return udtFilterThresholds;
        }

        /// <summary>
        /// Returns True if the there are multiple jobs in mDataPackagePeptideHitJobs for the dataset for the specified job
        /// </summary>
        /// <param name="intJob"></param>
        /// <returns>True if this job's dataset has multiple jobs in mDataPackagePeptideHitJobs, otherwise False</returns>
        /// <remarks></remarks>
        private bool JobFileRenameRequired(int intJob)
        {
            clsDataPackageJobInfo dataPkgJob = null;

            if (mDataPackagePeptideHitJobs.TryGetValue(intJob, out dataPkgJob))
            {
                string strDataset = dataPkgJob.Dataset;

                int intJobsForDataset = (from item in mDataPackagePeptideHitJobs where item.Value.Dataset == strDataset select item).ToList().Count();

                if (intJobsForDataset > 1)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }

            return false;
        }

        private bool LookupDataPackagePeptideHitJobs()
        {
            int intJob = 0;

            var dctDataPackageJobs = new Dictionary<int, clsDataPackageJobInfo>();

            if (mDataPackagePeptideHitJobs == null)
            {
                mDataPackagePeptideHitJobs = new Dictionary<int, clsDataPackageJobInfo>();
            }
            else
            {
                mDataPackagePeptideHitJobs.Clear();
            }

            if (!LoadDataPackageJobInfo(out dctDataPackageJobs))
            {
                var msg = "Error loading data package job info";
                LogError(msg + ": clsAnalysisToolRunnerBase.LoadDataPackageJobInfo() returned false");
                m_message = msg;
                return false;
            }

            var lstJobsToUse = ExtractPackedJobParameterList(clsAnalysisResourcesPRIDEConverter.JOB_PARAM_DATA_PACKAGE_PEPTIDE_HIT_JOBS);

            if (lstJobsToUse.Count == 0)
            {
                LogWarning("Packed job parameter " + clsAnalysisResourcesPRIDEConverter.JOB_PARAM_DATA_PACKAGE_PEPTIDE_HIT_JOBS +
                           " is empty; no jobs to process");
            }
            else
            {
                // Populate mDataPackagePeptideHitJobs using the jobs in lstJobsToUse and dctDataPackagePeptideHitJobs
                foreach (string strJob in lstJobsToUse)
                {
                    if (int.TryParse(strJob, out intJob))
                    {
                        clsDataPackageJobInfo dataPkgJob = null;
                        if (dctDataPackageJobs.TryGetValue(intJob, out dataPkgJob))
                        {
                            mDataPackagePeptideHitJobs.Add(intJob, dataPkgJob);
                        }
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// Parse the PRIDEConverter console output file to determine the PRIDE Version
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void ParseConsoleOutputFile(string strConsoleOutputFilePath)
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
                if (!File.Exists(strConsoleOutputFilePath))
                {
                    if (m_DebugLevel >= 4)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                            "Console output file not found: " + strConsoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing file " + strConsoleOutputFilePath);
                }

                string strLineIn = null;

                mConsoleOutputErrorMsg = string.Empty;

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    mConsoleOutputErrorMsg = string.Empty;

                    while (!srInFile.EndOfStream)
                    {
                        strLineIn = srInFile.ReadLine();

                        if (!string.IsNullOrWhiteSpace(strLineIn))
                        {
                            if (strLineIn.ToLower().Contains(" error "))
                            {
                                if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                                {
                                    mConsoleOutputErrorMsg = "Error running Pride Converter:";
                                }
                                mConsoleOutputErrorMsg += "; " + strLineIn;
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
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "Error parsing console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                    Console.WriteLine("Error parsing console output file (" + Path.GetFileName(strConsoleOutputFilePath) + ")");
                }
            }
        }

        private CloseOutType ProcessJob(KeyValuePair<int, clsDataPackageJobInfo> kvJobInfo, udtFilterThresholdsType udtFilterThresholds,
            clsAnalysisResults objAnalysisResults, string remoteTransferFolder, IReadOnlyDictionary<string, string> dctDatasetRawFilePaths,
            IReadOnlyDictionary<string, string> dctTemplateParameters, bool assumeInstrumentDataUnpurged)
        {
            bool blnSuccess = false;
            var resultFiles = new clsResultFileContainer();

            var intJob = kvJobInfo.Value.Job;
            var strDataset = kvJobInfo.Value.Dataset;

            if (mPreviousDatasetName != strDataset)
            {
                TransferPreviousDatasetFiles(objAnalysisResults, remoteTransferFolder);

                // Retrieve the dataset files for this dataset
                mPreviousDatasetName = strDataset;

                if (mCreatePrideXMLFiles & !mCreateMSGFReportFilesOnly)
                {
                    // Create the .mzXML files if it is missing
                    blnSuccess = CreateMzXMLFileIfMissing(strDataset, objAnalysisResults, dctDatasetRawFilePaths);
                    if (!blnSuccess)
                    {
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }
                }
            }

            // Update the cached analysis tool names
            if (!mSearchToolsUsed.Contains(kvJobInfo.Value.Tool))
            {
                mSearchToolsUsed.Add(kvJobInfo.Value.Tool);
            }

            // Update the cached NEWT info
            AddNEWTInfo(kvJobInfo.Value.Experiment_NEWT_ID, kvJobInfo.Value.Experiment_NEWT_Name);

            // Retrieve the PHRP files, MSGF+ results, and _dta.txt or .mzML.gz file for this job
            List<string> filesCopied = new List<string>();

            blnSuccess = RetrievePHRPFiles(intJob, strDataset, objAnalysisResults, remoteTransferFolder, filesCopied);
            if (!blnSuccess)
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            var searchedMzML = false;
            foreach (var copiedFile in filesCopied)
            {
                if (copiedFile.EndsWith(DOT_MZML, StringComparison.InvariantCultureIgnoreCase) ||
                    copiedFile.EndsWith(DOT_MZML_GZ, StringComparison.InvariantCultureIgnoreCase))
                {
                    searchedMzML = true;
                    break;
                }
            }

            resultFiles.MGFFilePath = string.Empty;
            if (mCreateMGFFiles && !searchedMzML)
            {
                // Convert the _dta.txt file to .mgf files
                string mgfPath;
                blnSuccess = ConvertCDTAToMGF(kvJobInfo.Value, out mgfPath);
                resultFiles.MGFFilePath = mgfPath;
                if (!blnSuccess)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            else
            {
                // Store the path to the _dta.txt or .mzML.gz file
                if (searchedMzML)
                {
                    resultFiles.MGFFilePath = Path.Combine(m_WorkDir, strDataset + DOT_MZML_GZ);
                }
                else
                {
                    resultFiles.MGFFilePath = Path.Combine(m_WorkDir, strDataset + "_dta.txt");
                }

                if (!assumeInstrumentDataUnpurged && !searchedMzML && !File.Exists(resultFiles.MGFFilePath))
                {
                    // .mgf file not found
                    // We don't check for .mzML.gz files since those are not copied locally if they already exist in remoteTransferFolder
                    resultFiles.MGFFilePath = string.Empty;
                }
            }

            // Update the .mzID file(s) for this job
            // Gzip after updating

            if (mProcessMzIdFiles && kvJobInfo.Value.PeptideHitResultType == clsPHRPReader.ePeptideHitResultType.MSGFDB)
            {
                m_message = string.Empty;

                List<string> mzIdFilePaths = null;
                blnSuccess = UpdateMzIdFiles(kvJobInfo.Value, searchedMzML, out mzIdFilePaths, dctTemplateParameters);

                if (!blnSuccess || mzIdFilePaths == null || mzIdFilePaths.Count == 0)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        LogError("UpdateMzIdFiles returned false for job " + intJob + ", dataset " + strDataset);
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                resultFiles.MzIDFilePaths.Clear();

                foreach (var mzidFilePath in mzIdFilePaths)
                {
                    var mzidFile = new FileInfo(mzidFilePath);

                    // Note that the original file will be auto-deleted after the .gz file is created
                    var gzippedMZidFile = GZipFile(mzidFile);

                    if (gzippedMZidFile == null)
                    {
                        if (string.IsNullOrEmpty(m_message))
                        {
                            LogError("GZipFile returned false for " + mzidFilePath);
                        }
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    resultFiles.MzIDFilePaths.Add(gzippedMZidFile.FullName);
                }
            }

            if (mIncludePepXMLFiles && kvJobInfo.Value.PeptideHitResultType != clsPHRPReader.ePeptideHitResultType.Unknown)
            {
                var pepXmlFilename = kvJobInfo.Value.Dataset + ".pepXML";
                var pepXMLFile = new FileInfo(Path.Combine(m_WorkDir, pepXmlFilename));
                if (pepXMLFile.Exists)
                {
                    // Make sure it is capitalized correctly, then gzip it

                    if (!string.Equals(pepXMLFile.Name, pepXmlFilename, StringComparison.InvariantCulture))
                    {
                        pepXMLFile.MoveTo(pepXMLFile.FullName + ".tmp");
                        Thread.Sleep(50);
                        pepXMLFile.MoveTo(Path.Combine(m_WorkDir, pepXmlFilename));
                        Thread.Sleep(50);
                    }

                    // Note that the original file will be auto-deleted after the .gz file is created
                    var gzippedMZidFile = GZipFile(pepXMLFile);

                    if (gzippedMZidFile == null)
                    {
                        if (string.IsNullOrEmpty(m_message))
                        {
                            LogError("GZipFile returned false for " + pepXMLFile.FullName);
                        }
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    resultFiles.PepXMLFile = gzippedMZidFile.FullName;
                }
            }

            // Store the instrument group and instrument name
            StoreInstrumentInfo(kvJobInfo.Value);

            resultFiles.PrideXmlFilePath = string.Empty;

            if (mCreatePrideXMLFiles)
            {
                // Create the .msgf-report.xml file for this job
                string strPrideReportXMLFilePath = string.Empty;
                blnSuccess = CreateMSGFReportFile(intJob, strDataset, udtFilterThresholds, out strPrideReportXMLFilePath);
                if (!blnSuccess)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                AddToListIfNew(mPreviousDatasetFilesToDelete, strPrideReportXMLFilePath);

                if (!mCreateMSGFReportFilesOnly)
                {
                    // Create the .msgf-Pride.xml file for this job
                    string prideXmlPath;
                    blnSuccess = CreatePrideXMLFile(intJob, strDataset, strPrideReportXMLFilePath, out prideXmlPath);
                    resultFiles.PrideXmlFilePath = prideXmlPath;
                    if (!blnSuccess)
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }

            blnSuccess = AppendToPXFileInfo(kvJobInfo.Value, dctDatasetRawFilePaths, resultFiles);

            if (blnSuccess)
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            else
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private string PXFileTypeName(clsPXFileInfo.ePXFileType ePXFileType)
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

            string strTemplateFileName = null;
            string strTemplateFilePath = null;
            string strLineIn = null;

            var dctParameters = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

            var dctKeyNameOverrides = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
            dctKeyNameOverrides.Add("name", "submitter_name");
            dctKeyNameOverrides.Add("email", "submitter_email");
            dctKeyNameOverrides.Add("affiliation", "submitter_affiliation");
            dctKeyNameOverrides.Add("title", "project_title");
            dctKeyNameOverrides.Add("description", "project_description");
            dctKeyNameOverrides.Add("type", "submission_type");
            dctKeyNameOverrides.Add("comment", OBSOLETE_FIELD_FLAG);
            dctKeyNameOverrides.Add("pride_login", "submitter_pride_login");
            dctKeyNameOverrides.Add("pubmed", "pubmed_id");

            try
            {
                strTemplateFileName = clsAnalysisResourcesPRIDEConverter.GetPXSubmissionTemplateFilename(m_jobParams, WarnIfJobParamMissing: false);
                strTemplateFilePath = Path.Combine(m_WorkDir, strTemplateFileName);

                if (!File.Exists(strTemplateFilePath))
                {
                    return dctParameters;
                }

                using (var srTemplateFile = new StreamReader(new FileStream(strTemplateFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srTemplateFile.EndOfStream)
                    {
                        strLineIn = srTemplateFile.ReadLine();

                        if (!string.IsNullOrEmpty(strLineIn))
                        {
                            if (strLineIn.StartsWith("MTD"))
                            {
                                var lstColumns = strLineIn.Split(new char[] { '\t' }, 3).ToList();

                                if (lstColumns.Count >= 3 && !string.IsNullOrEmpty(lstColumns[1]))
                                {
                                    var keyName = lstColumns[1];

                                    // Automatically rename parameters updated from v1.x to v2.x of the .px file format
                                    string keyNameNew = string.Empty;
                                    if (dctKeyNameOverrides.TryGetValue(keyName, out keyNameNew))
                                    {
                                        keyName = keyNameNew;
                                    }

                                    if (!string.Equals(keyName, OBSOLETE_FIELD_FLAG) && !dctParameters.ContainsKey(keyName))
                                    {
                                        dctParameters.Add(keyName, lstColumns[2]);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Error in ReadTemplatePXSubmissionFile", ex);
                return dctParameters;
            }

            return dctParameters;
        }

        private clsSampleMetadata.udtCvParamInfoType ReadWriteCvParam(XmlTextReader objXmlReader, XmlTextWriter objXmlWriter,
            Stack<int> lstElementCloseDepths)
        {
            var udtCvParam = new clsSampleMetadata.udtCvParamInfoType();
            udtCvParam.Clear();

            objXmlWriter.WriteStartElement(objXmlReader.Name);

            if (objXmlReader.HasAttributes)
            {
                objXmlReader.MoveToFirstAttribute();
                do
                {
                    objXmlWriter.WriteAttributeString(objXmlReader.Name, objXmlReader.Value);

                    switch (objXmlReader.Name)
                    {
                        case "accession":
                            udtCvParam.Accession = objXmlReader.Value;
                            break;
                        case "cvRef":
                            udtCvParam.CvRef = objXmlReader.Value;
                            break;
                        case "name":
                            udtCvParam.Name = objXmlReader.Value;
                            break;
                        case "value":
                            udtCvParam.Value = objXmlReader.Value;
                            break;
                        case "unitCvRef":
                            udtCvParam.unitCvRef = objXmlReader.Value;
                            break;
                        case "unitName":
                            udtCvParam.unitName = objXmlReader.Value;
                            break;
                        case "unitAccession":
                            udtCvParam.unitAccession = objXmlReader.Value;
                            break;
                    }
                } while (objXmlReader.MoveToNextAttribute());

                lstElementCloseDepths.Push(objXmlReader.Depth);
            }
            else if (objXmlReader.IsEmptyElement)
            {
                objXmlWriter.WriteEndElement();
            }

            return udtCvParam;
        }

        private bool RetrievePHRPFiles(int intJob, string strDataset, clsAnalysisResults objAnalysisResults, string remoteTransferFolder,
            ICollection<string> filesCopied)
        {
            string strJobInfoFilePath = null;
            var lstFilesToCopy = new List<string>();

            try
            {
                strJobInfoFilePath = clsDataPackageFileHandler.GetJobInfoFilePath(intJob, m_WorkDir);

                if (!File.Exists(strJobInfoFilePath))
                {
                    // Assume all of the files already exist
                    return true;
                }

                // Read the contents of the JobInfo file
                // It will be empty if no PHRP files are required
                using (var srInFile = new StreamReader(new FileStream(strJobInfoFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        lstFilesToCopy.Add(srInFile.ReadLine());
                    }
                }

                var fileCountNotFound = 0;

                // Retrieve the files
                // If the same dataset has multiple jobs then we might overwrite existing files;
                //   that's OK since results files that we care about will have been auto-renamed based on the call to JobFileRenameRequired

                foreach (string sourceFilePath in lstFilesToCopy)
                {
                    if (sourceFilePath.StartsWith(clsAnalysisResources.MYEMSL_PATH_FLAG))
                    {
                        // Make sure the myEMSLUtilities object knows about this dataset
                        m_MyEMSLUtilities.AddDataset(strDataset);
                        string cleanFilePath = null;
                        DatasetInfoBase.ExtractMyEMSLFileID(sourceFilePath, out cleanFilePath);

                        var fiSourceFileClean = new FileInfo(cleanFilePath);
                        var unzipRequired = (fiSourceFileClean.Extension.ToLower() == ".zip" ||
                                             fiSourceFileClean.Extension.ToLower() == clsAnalysisResources.DOT_GZ_EXTENSION.ToLower());

                        m_MyEMSLUtilities.AddFileToDownloadQueue(sourceFilePath, unzipRequired);

                        filesCopied.Add(fiSourceFileClean.Name);

                        continue;
                    }

                    var fiSourceFile = new FileInfo(sourceFilePath);

                    if (!fiSourceFile.Exists)
                    {
                        fileCountNotFound += 1;
                        LogError(string.Format("File not found for job {0}: {1}", intJob, sourceFilePath));
                        continue;
                    }

                    var targetFilePath = Path.Combine(m_WorkDir, fiSourceFile.Name);

                    var fiLocalFile = new FileInfo(targetFilePath);
                    var alreadyCopiedToTransferDirectory = false;

                    if (fiSourceFile.Name.EndsWith(DOT_MZML, StringComparison.InvariantCultureIgnoreCase) ||
                        fiSourceFile.Name.EndsWith(DOT_MZML_GZ, StringComparison.InvariantCultureIgnoreCase))
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
                        objAnalysisResults.CopyFileWithRetry(fiSourceFile.FullName, fiLocalFile.FullName, true);

                        if (!fiLocalFile.Exists)
                        {
                            LogError("PHRP file was not copied locally: " + fiLocalFile.Name);
                            return false;
                        }

                        filesCopied.Add(fiSourceFile.Name);

                        var blnUnzipped = false;

                        if (fiLocalFile.Extension.ToLower() == ".zip")
                        {
                            // Decompress the .zip file
                            m_IonicZipTools.UnzipFile(fiLocalFile.FullName, m_WorkDir);
                            blnUnzipped = true;
                        }
                        else if (fiLocalFile.Extension.ToLower() == clsAnalysisResources.DOT_GZ_EXTENSION.ToLower())
                        {
                            // Decompress the .gz file
                            m_IonicZipTools.GUnzipFile(fiLocalFile.FullName, m_WorkDir);
                            blnUnzipped = true;
                        }

                        if (blnUnzipped)
                        {
                            foreach (var kvUnzippedFile in m_IonicZipTools.MostRecentUnzippedFiles)
                            {
                                filesCopied.Add(kvUnzippedFile.Key);
                                AddToListIfNew(mPreviousDatasetFilesToDelete, kvUnzippedFile.Value);
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

        private bool RetrieveStoragePathInfoTargetFile(string strStoragePathInfoFilePath, clsAnalysisResults objAnalysisResults,
            out string strDestPath)
        {
            return RetrieveStoragePathInfoTargetFile(strStoragePathInfoFilePath, objAnalysisResults, IsFolder: false, strDestPath: out strDestPath);
        }

        private bool RetrieveStoragePathInfoTargetFile(string strStoragePathInfoFilePath, clsAnalysisResults objAnalysisResults, bool IsFolder,
            out string strDestPath)
        {
            string strSourceFilePath = string.Empty;

            strDestPath = string.Empty;

            try
            {
                if (!File.Exists(strStoragePathInfoFilePath))
                {
                    var msg = "StoragePathInfo file not found";
                    LogError(msg + ": " + strStoragePathInfoFilePath);
                    m_message = msg;
                    return false;
                }

                using (var srInfoFile = new StreamReader(new FileStream(strStoragePathInfoFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    if (!srInfoFile.EndOfStream)
                    {
                        strSourceFilePath = srInfoFile.ReadLine();
                    }
                }

                if (string.IsNullOrEmpty(strSourceFilePath))
                {
                    var msg = "StoragePathInfo file was empty";
                    LogError(msg + ": " + strStoragePathInfoFilePath);
                    m_message = msg;
                    return false;
                }

                strDestPath = Path.Combine(m_WorkDir, Path.GetFileName(strSourceFilePath));

                if (IsFolder)
                {
                    objAnalysisResults.CopyDirectory(strSourceFilePath, strDestPath, Overwrite: true);
                }
                else
                {
                    objAnalysisResults.CopyFileWithRetry(strSourceFilePath, strDestPath, Overwrite: true);
                }
            }
            catch (Exception ex)
            {
                LogError("Error in RetrieveStoragePathInfoTargetFile", ex);
                return false;
            }

            return true;
        }

        private bool RunPrideConverter(int intJob, string strDataset, string strMsgfResultsFilePath, string strMzXMLFilePath, string strPrideReportFilePath)
        {
            string CmdStr = null;

            if (string.IsNullOrEmpty(strMsgfResultsFilePath))
            {
                LogError("strMsgfResultsFilePath has not been defined; unable to continue");
                return false;
            }

            if (string.IsNullOrEmpty(strMzXMLFilePath))
            {
                LogError("strMzXMLFilePath has not been defined; unable to continue");
                return false;
            }

            if (string.IsNullOrEmpty(strPrideReportFilePath))
            {
                LogError("strPrideReportFilePath has not been defined; unable to continue");
                return false;
            }

            mCmdRunner = new clsRunDosProgram(m_WorkDir);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            if (m_DebugLevel >= 1)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
                    "Running PrideConverter on " + Path.GetFileName(strMsgfResultsFilePath));
            }

            m_StatusTools.CurrentOperation = "Running PrideConverter";
            m_StatusTools.UpdateAndWrite(m_progress);

            CmdStr = "-jar " + PossiblyQuotePath(mPrideConverterProgLoc);

            CmdStr += " -converter -mode convert -engine msgf -sourcefile " + PossiblyQuotePath(strMsgfResultsFilePath);     // QC_Shew_12_02_Run-03_18Jul12_Roc_12-04-08.msgf
            CmdStr += " -spectrafile " + PossiblyQuotePath(strMzXMLFilePath);                                                // QC_Shew_12_02_Run-03_18Jul12_Roc_12-04-08.mzXML
            CmdStr += " -reportfile " + PossiblyQuotePath(strPrideReportFilePath);                                           // QC_Shew_12_02_Run-03_18Jul12_Roc_12-04-08.msgf-report.xml
            CmdStr += " -reportOnlyIdentifiedSpectra";
            CmdStr += " -debug";

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, mJavaProgLoc + " " + CmdStr);

            mCmdRunner.CreateNoWindow = false;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = false;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, PRIDEConverter_CONSOLE_OUTPUT);
            mCmdRunner.WorkDir = m_WorkDir;

            bool blnSuccess = false;
            blnSuccess = mCmdRunner.RunProgram(mJavaProgLoc, CmdStr, "PrideConverter", true);

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
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg);
                    Console.WriteLine(mConsoleOutputErrorMsg);
                }
            }

            if (!blnSuccess)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Error running PrideConverter, dataset " + strDataset + ", job " + intJob);
                if (string.IsNullOrWhiteSpace(m_message))
                {
                    m_message = "Error running PrideConverter";
                    Console.WriteLine(m_message);
                }
            }

            return blnSuccess;
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
            List<string> lstInstruments = null;
            if (mInstrumentGroupsStored.TryGetValue(instrumentGroup, out lstInstruments))
            {
                if (!lstInstruments.Contains(instrumentName))
                {
                    lstInstruments.Add(instrumentName);
                }
            }
            else
            {
                lstInstruments = new List<string> { instrumentName };
                mInstrumentGroupsStored.Add(instrumentGroup, lstInstruments);
            }
        }

        private void StoreMzIdSampleInfo(string strMzIdFilePath, clsSampleMetadata sampleMetadata)
        {
            var fiFile = new FileInfo(strMzIdFilePath);

            if (!mMzIdSampleInfo.ContainsKey(fiFile.Name))
            {
                mMzIdSampleInfo.Add(fiFile.Name, sampleMetadata);
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <param name="strPrideConverterProgLoc"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo(string strPrideConverterProgLoc)
        {
            string strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");
            }

            // Store paths to key files in ioToolFiles
            List<FileInfo> ioToolFiles = new List<FileInfo>();

            if (mCreatePrideXMLFiles)
            {
                var fiPrideConverter = new FileInfo(strPrideConverterProgLoc);
                if (!fiPrideConverter.Exists)
                {
                    try
                    {
                        strToolVersionInfo = "Unknown";
                        return base.SetStepTaskToolVersion(strToolVersionInfo, new List<FileInfo>());
                    }
                    catch (Exception ex)
                    {
                        var msg = "Exception calling SetStepTaskToolVersion: " + ex.Message;
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                        Console.WriteLine(msg);
                        return false;
                    }
                }

                // Run the PRIDE Converter using the -version switch to determine its version
                strToolVersionInfo = GetPrideConverterVersion(fiPrideConverter.FullName);

                ioToolFiles.Add(fiPrideConverter);
            }
            else
            {
                // Lookup the version of the AnalysisManagerPrideConverter plugin
                if (!StoreToolVersionInfoForLoadedAssembly(ref strToolVersionInfo, "AnalysisManagerPRIDEConverterPlugIn", blnIncludeRevision: false))
                {
                    return false;
                }
            }

            ioToolFiles.Add(new FileInfo(mMSXmlGeneratorAppPath));

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        private void TransferPreviousDatasetFiles(clsAnalysisResults objAnalysisResults, string remoteTransferFolder)
        {
            // Delete the dataset files for the previous dataset
            var lstFilesToRetry = new List<string>();

            if (mPreviousDatasetFilesToCopy.Count > 0)
            {
                lstFilesToRetry.Clear();

                try
                {
                    // Copy the files we want to keep to the remote Transfer Directory
                    foreach (var strSrcFilePath in mPreviousDatasetFilesToCopy)
                    {
                        string strTargetFilePath = Path.Combine(remoteTransferFolder, Path.GetFileName(strSrcFilePath));

                        if (File.Exists(strSrcFilePath))
                        {
                            try
                            {
                                objAnalysisResults.CopyFileWithRetry(strSrcFilePath, strTargetFilePath, true);
                                AddToListIfNew(mPreviousDatasetFilesToDelete, strSrcFilePath);
                            }
                            catch (Exception ex)
                            {
                                LogError("Exception copying file to transfer directory", ex);
                                lstFilesToRetry.Add(strSrcFilePath);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    // Folder creation error
                    LogError("Exception copying files to " + remoteTransferFolder, ex);
                    lstFilesToRetry.AddRange(mPreviousDatasetFilesToCopy);
                }

                mPreviousDatasetFilesToCopy.Clear();

                if (lstFilesToRetry.Count > 0)
                {
                    mPreviousDatasetFilesToCopy.AddRange(lstFilesToRetry);

                    foreach (var item in lstFilesToRetry)
                    {
                        if (mPreviousDatasetFilesToDelete.Contains(item, StringComparer.CurrentCultureIgnoreCase))
                        {
                            mPreviousDatasetFilesToDelete.Remove(item);
                        }
                    }
                }
            }

            if (mPreviousDatasetFilesToDelete.Count > 0)
            {
                lstFilesToRetry.Clear();

                foreach (var item in mPreviousDatasetFilesToDelete)
                {
                    try
                    {
                        if (File.Exists(item))
                        {
                            File.Delete(item);
                        }
                    }
                    catch (Exception ex)
                    {
                        lstFilesToRetry.Add(item);
                    }
                }

                mPreviousDatasetFilesToDelete.Clear();

                if (lstFilesToRetry.Count > 0)
                {
                    mPreviousDatasetFilesToDelete.AddRange(lstFilesToRetry);
                }
            }
        }

        private eMSGFReportXMLFileLocation UpdateMSGFReportXMLFileLocation(eMSGFReportXMLFileLocation eFileLocation, string strElementName, bool blnInsideMzDataDescription)
        {
            switch (strElementName)
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
                    if (blnInsideMzDataDescription)
                    {
                        eFileLocation = eMSGFReportXMLFileLocation.MzDataAdmin;
                    }
                    break;
                case "instrument":
                    if (blnInsideMzDataDescription)
                    {
                        eFileLocation = eMSGFReportXMLFileLocation.MzDataInstrument;
                    }
                    break;
                case "dataProcessing":
                    if (blnInsideMzDataDescription)
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
        /// Update the .mzid file for the given job and dataset to have the correct Accession value for FileFormat
        /// Also update attributes location and name for element SpectraData if we converted _dta.txt files to .mgf files
        /// </summary>
        /// <param name="dataPkgJob">Data package job info</param>
        /// <param name="searchedMzML">True if analysis job used a .mzML file (though we track .mzml.gz files with this class)</param>
        /// <param name="mzIdFilePaths">Output parameter: path to the .mzid file for this job (will be multiple files if a SplitFasta search was performed)</param>
        /// <param name="dctTemplateParameters"></param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        private bool UpdateMzIdFiles(clsDataPackageJobInfo dataPkgJob, bool searchedMzML, out List<string> mzIdFilePaths,
            IReadOnlyDictionary<string, string> dctTemplateParameters)
        {
            var sampleMetadata = new clsSampleMetadata();
            sampleMetadata.Clear();

            sampleMetadata.Species = GetNEWTCv(dataPkgJob.Experiment_NEWT_ID, dataPkgJob.Experiment_NEWT_Name);
            sampleMetadata.Tissue = GetValueOrDefault("tissue", dctTemplateParameters, DEFAULT_TISSUE_CV);

            string value = string.Empty;

            if ((dctTemplateParameters.TryGetValue("cell_type", out value)))
            {
                sampleMetadata.CellType = value;
            }
            else
            {
                sampleMetadata.CellType = string.Empty;
            }

            if ((dctTemplateParameters.TryGetValue("disease", out value)))
            {
                sampleMetadata.Disease = value;
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
                // Open each .mzid and parse it to create a new .mzid file
                // Use a forward-only XML reader, copying most of the elements verbatim, but customizing some of them

                // For _dta.txt files, use <cvParam accession="MS:1001369" cvRef="PSI-MS" name="text file"/>
                // For .mgf files,     use <cvParam accession="MS:1001062" cvRef="PSI-MS" name="Mascot MGF file"/>
                // Will also need to update the location and name attributes of the SpectraData element
                // <SpectraData location="E:\DMS_WorkDir3\QC_Shew_08_04-pt5-2_11Jan09_Sphinx_08-11-18_dta.txt" name="QC_Shew_08_04-pt5-2_11Jan09_Sphinx_08-11-18_dta.txt" id="SID_1">

                // For split FASTA files each job step should have a custom .FASTA file, but we're ignoring that fact for now

                bool success = false;
                string strMzIDFilePath = null;

                if (dataPkgJob.NumberOfClonedSteps > 0)
                {
                    for (var splitFastaResultID = 1; splitFastaResultID <= dataPkgJob.NumberOfClonedSteps; splitFastaResultID++)
                    {
                        success = UpdateMzIdFile(dataPkgJob.Job, dataPkgJob.Dataset, searchedMzML, splitFastaResultID, sampleMetadata, out strMzIDFilePath);
                        if (success)
                        {
                            mzIdFilePaths.Add(strMzIDFilePath);
                        }
                        else
                        {
                            break;
                        }
                    }
                }
                else
                {
                    success = UpdateMzIdFile(dataPkgJob.Job, dataPkgJob.Dataset, searchedMzML, 0, sampleMetadata, out strMzIDFilePath);
                    if (success)
                    {
                        mzIdFilePaths.Add(strMzIDFilePath);
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

                return false;
            }
        }

        /// <summary>
        /// Update a single .mzid file to have the correct Accession value for FileFormat
        /// Also update attributes location and name for element SpectraData if we converted _dta.txt files to .mgf files
        /// </summary>
        /// <param name="dataPkgJob">Data package job</param>
        /// <param name="dataPkgDataset">Data package dataset</param>
        /// <param name="searchedMzML">True if analysis job used a .mzML file (though we track .mzml.gz files with this class)</param>
        /// <param name="splitFastaResultID">For SplitFasta jobs, the part number being processed; 0 for non-SplitFasta jobs</param>
        /// <param name="sampleMetadata">Sample Metadata</param>
        /// <param name="strMzIDFilePath">Output parameter: path to the .mzid file being processed</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        private bool UpdateMzIdFile(int dataPkgJob, string dataPkgDataset, bool searchedMzML, int splitFastaResultID, clsSampleMetadata sampleMetadata, out string strMzIDFilePath)
        {
            bool nodeWritten = false;
            bool skipNode = false;
            var readModAccession = false;
            var readingSpecificityRules = false;

            var lstAttributeOverride = new Dictionary<string, string>();

            var lstElementCloseDepths = new Stack<int>();

            var eFileLocation = eMzIDXMLFileLocation.Header;
            var lstRecentElements = new Queue<string>();

            try
            {
                string strSourceFileName = null;
                string strUpdatedFilePathTemp = null;
                var filePartText = string.Empty;

                if (splitFastaResultID > 0)
                {
                    filePartText = "_Part" + splitFastaResultID;
                }

                // First look for a job-specific version of the .mzid file
                strSourceFileName = "Job" + dataPkgJob.ToString() + "_" + dataPkgDataset + "_msgfplus" + filePartText + ".mzid";
                strMzIDFilePath = Path.Combine(m_WorkDir, strSourceFileName);

                if (!File.Exists(strMzIDFilePath))
                {
                    // Job-specific version not found
                    // Look for one that simply starts with the dataset name
                    strSourceFileName = dataPkgDataset + "_msgfplus" + filePartText + ".mzid";
                    strMzIDFilePath = Path.Combine(m_WorkDir, strSourceFileName);

                    if (!File.Exists(strMzIDFilePath))
                    {
                        LogError("MzID file not found for job " + dataPkgJob + ": " + strSourceFileName);
                        return false;
                    }
                }

                AddToListIfNew(mPreviousDatasetFilesToDelete, strMzIDFilePath);
                AddToListIfNew(mPreviousDatasetFilesToDelete, strMzIDFilePath + DOT_GZ);

                strUpdatedFilePathTemp = strMzIDFilePath + ".tmp";
                var replaceOriginal = false;

                // Important: instantiate the XmlTextWriter using an instance of the UTF8Encoding class where the byte order mark (BOM) is not emitted
                // The ProteomeXchange import pipeline breaks if the .mzid files have the BOM at the start of the file
                // Note that the following Using command will not work if the .mzid file has an encoding string of <?xml version="1.0" encoding="Cp1252"?>
                // using (var objXmlReader = new XmlTextReader(new FileStream(strMzIDFilePath, FileMode.Open, FileAccess.Read)))
                // Thus, we instead first instantiate a streamreader using explicit encodings
                // Then instantiate the XmlTextReader
                using (var objXmlWriter = new XmlTextWriter(new FileStream(strUpdatedFilePathTemp, FileMode.Create, FileAccess.Write, FileShare.Read), new UTF8Encoding(false)))
                using (var srSourceFile = new StreamReader(new FileStream(strMzIDFilePath, FileMode.Open, FileAccess.Read, FileShare.Read), Encoding.GetEncoding("ISO-8859-1")))
                using (var objXmlReader = new XmlTextReader(srSourceFile))
                {
                    objXmlWriter.Formatting = Formatting.Indented;
                    objXmlWriter.Indentation = 2;

                    objXmlWriter.WriteStartDocument();

                    while (objXmlReader.Read())
                    {
                        switch (objXmlReader.NodeType)
                        {
                            case XmlNodeType.Whitespace:
                                break;
                            // Skip whitespace since the writer should be auto-formatting things
                            // objXmlWriter.WriteWhitespace(objXmlReader.Value)

                            case XmlNodeType.Comment:
                                objXmlWriter.WriteComment(objXmlReader.Value);

                                break;
                            case XmlNodeType.Element:
                                // Start element

                                if (lstRecentElements.Count > 10)
                                    lstRecentElements.Dequeue();
                                lstRecentElements.Enqueue("Element " + objXmlReader.Name);

                                while (lstElementCloseDepths.Count > 0 && lstElementCloseDepths.Peek() > objXmlReader.Depth)
                                {
                                    lstElementCloseDepths.Pop();

                                    objXmlWriter.WriteEndElement();
                                }

                                eFileLocation = UpdateMZidXMLFileLocation(eFileLocation, objXmlReader.Name);

                                nodeWritten = false;
                                skipNode = false;

                                lstAttributeOverride.Clear();

                                switch (objXmlReader.Name)
                                {
                                    case "SpectraData":
                                        if (searchedMzML)
                                        {
                                            // MSGF+ will list an .mzML file here
                                            // Although we upload .mzML.gz files, the .mzid file needs to list the input file as .mzML
                                            // Thus, do not update the .mzid file
                                        }
                                        else
                                        {
                                            // Override the location and name attributes for this node
                                            string strSpectraDataFilename = null;

                                            if (mCreateMGFFiles)
                                            {
                                                strSpectraDataFilename = dataPkgDataset + ".mgf";
                                            }
                                            else
                                            {
                                                strSpectraDataFilename = dataPkgDataset + "_dta.txt";
                                            }

                                            lstAttributeOverride.Add("location", "C:\\DMS_WorkDir\\" + strSpectraDataFilename);
                                            lstAttributeOverride.Add("name", strSpectraDataFilename);
                                        }

                                        break;
                                    case "FileFormat":

                                        if (eFileLocation == eMzIDXMLFileLocation.InputSpectraData & !searchedMzML)
                                        {
                                            // Override the accession and name attributes for this node

                                            // For .mzML files, the .mzID file should already have:
                                            //                         <cvParam accession="MS:1000584" cvRef="PSI-MS" name="mzML file"/>
                                            // For .mgf files,     use <cvParam accession="MS:1001062" cvRef="PSI-MS" name="Mascot MGF file"/>
                                            // For _dta.txt files, use <cvParam accession="MS:1001369" cvRef="PSI-MS" name="text file"/>

                                            string strAccession = null;
                                            string strFormatName = null;

                                            if (mCreateMGFFiles)
                                            {
                                                strAccession = "MS:1001062";
                                                strFormatName = "Mascot MGF file";
                                            }
                                            else
                                            {
                                                strAccession = "MS:1001369";
                                                strFormatName = "text file";
                                            }

                                            objXmlWriter.WriteStartElement("FileFormat");
                                            objXmlWriter.WriteStartElement("cvParam");

                                            objXmlWriter.WriteAttributeString("accession", strAccession);
                                            objXmlWriter.WriteAttributeString("cvRef", "PSI-MS");
                                            objXmlWriter.WriteAttributeString("name", strFormatName);

                                            objXmlWriter.WriteEndElement();  // cvParam
                                            objXmlWriter.WriteEndElement();  // FileFormat

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
                                        if (readModAccession & !readingSpecificityRules)
                                        {
                                            var udtModInfo = ReadWriteCvParam(objXmlReader, objXmlWriter, lstElementCloseDepths);

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
                                    if (objXmlReader.NodeType != XmlNodeType.EndElement)
                                    {
                                        // Skip this element (and any children nodes enclosed in this elemnt)
                                        // Likely should not do this when objXmlReader.NodeType is XmlNodeType.EndElement
                                        objXmlReader.Skip();
                                    }
                                    replaceOriginal = true;
                                }
                                else if (!nodeWritten)
                                {
                                    // Copy this element from the source file to the target file

                                    objXmlWriter.WriteStartElement(objXmlReader.Name);

                                    if (objXmlReader.HasAttributes)
                                    {
                                        objXmlReader.MoveToFirstAttribute();
                                        do
                                        {
                                            string strAttributeOverride = string.Empty;
                                            if (lstAttributeOverride.Count > 0 && lstAttributeOverride.TryGetValue(objXmlReader.Name, out strAttributeOverride))
                                            {
                                                objXmlWriter.WriteAttributeString(objXmlReader.Name, strAttributeOverride);
                                                replaceOriginal = true;
                                            }
                                            else
                                            {
                                                objXmlWriter.WriteAttributeString(objXmlReader.Name, objXmlReader.Value);
                                            }
                                        } while (objXmlReader.MoveToNextAttribute());

                                        lstElementCloseDepths.Push(objXmlReader.Depth);
                                    }
                                    else if (objXmlReader.IsEmptyElement)
                                    {
                                        objXmlWriter.WriteEndElement();
                                    }
                                }

                                break;
                            case XmlNodeType.EndElement:

                                if (lstRecentElements.Count > 10)
                                    lstRecentElements.Dequeue();
                                lstRecentElements.Enqueue("EndElement " + objXmlReader.Name);

                                while (lstElementCloseDepths.Count > 0 && lstElementCloseDepths.Peek() > objXmlReader.Depth + 1)
                                {
                                    lstElementCloseDepths.Pop();
                                    objXmlWriter.WriteEndElement();
                                }

                                objXmlWriter.WriteEndElement();

                                while (lstElementCloseDepths.Count > 0 && lstElementCloseDepths.Peek() > objXmlReader.Depth)
                                {
                                    lstElementCloseDepths.Pop();
                                }

                                if (objXmlReader.Name == "SearchModification")
                                {
                                    readModAccession = false;
                                }

                                if (objXmlReader.Name == "SpecificityRules")
                                {
                                    readingSpecificityRules = false;
                                }

                                break;
                            case XmlNodeType.Text:

                                if (!string.IsNullOrEmpty(objXmlReader.Value))
                                {
                                    if (lstRecentElements.Count > 10)
                                        lstRecentElements.Dequeue();
                                    if (objXmlReader.Value.Length > 10)
                                    {
                                        lstRecentElements.Enqueue(objXmlReader.Value.Substring(0, 10));
                                    }
                                    else
                                    {
                                        lstRecentElements.Enqueue(objXmlReader.Value);
                                    }
                                }

                                objXmlWriter.WriteString(objXmlReader.Value);

                                break;
                        }
                    }

                    objXmlWriter.WriteEndDocument();
                }

                // Must append .gz to the .mzid file name to allow for successful lookups in function CreatePXSubmissionFile
                StoreMzIdSampleInfo(strMzIDFilePath + DOT_GZ, sampleMetadata);

                Thread.Sleep(250);
                PRISM.Processes.clsProgRunner.GarbageCollectNow();

                if (!replaceOriginal)
                {
                    // Nothing was changed; delete the .tmp file
                    File.Delete(strUpdatedFilePathTemp);
                    return true;
                }

                try
                {
                    // Replace the original .mzid file with the updated one
                    File.Delete(strMzIDFilePath);

                    if (JobFileRenameRequired(dataPkgJob))
                    {
                        strMzIDFilePath = Path.Combine(m_WorkDir, dataPkgDataset + "_Job" + dataPkgJob.ToString() + "_msgfplus.mzid");
                    }
                    else
                    {
                        strMzIDFilePath = Path.Combine(m_WorkDir, dataPkgDataset + "_msgfplus.mzid");
                    }

                    File.Move(strUpdatedFilePathTemp, strMzIDFilePath);
                }
                catch (Exception ex)
                {
                    LogError("Exception replacing the original .mzID file with the updated one for job " + dataPkgJob + ", dataset " + dataPkgDataset, ex);
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception in UpdateMzIdFile for job " + dataPkgJob + ", dataset " + dataPkgDataset, ex);

                string strRecentElements = string.Empty;
                foreach (var strItem in lstRecentElements)
                {
                    if (string.IsNullOrEmpty(strRecentElements))
                    {
                        strRecentElements = string.Copy(strItem);
                    }
                    else
                    {
                        strRecentElements += "; " + strItem;
                    }
                }

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strRecentElements);

                strMzIDFilePath = string.Empty;
                return false;
            }
        }

        private eMzIDXMLFileLocation UpdateMZidXMLFileLocation(eMzIDXMLFileLocation eFileLocation, string strElementName)
        {
            switch (strElementName)
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

        private void WriteConfigurationOption(XmlTextWriter objXmlWriter, string KeyName, string Value)
        {
            objXmlWriter.WriteStartElement("Option");
            objXmlWriter.WriteElementString("Key", KeyName);
            objXmlWriter.WriteElementString("Value", Value);
            objXmlWriter.WriteEndElement();
        }

        /// <summary>
        /// Append a new header line to the .px file
        /// </summary>
        /// <param name="swPXFile"></param>
        /// <param name="strType"></param>
        /// <param name="strValue"></param>
        /// <remarks></remarks>
        private void WritePXHeader(StreamWriter swPXFile, string strType, string strValue)
        {
            WritePXHeader(swPXFile, strType, strValue, new Dictionary<string, string>());
        }

        /// <summary>
        /// Append a new header line to the .px file
        /// </summary>
        /// <param name="swPXFile"></param>
        /// <param name="strType"></param>
        /// <param name="strValue"></param>
        /// <param name="dctParameters"></param>
        /// <remarks></remarks>
        private void WritePXHeader(StreamWriter swPXFile, string strType, string strValue, IReadOnlyDictionary<string, string> dctParameters)
        {
            WritePXHeader(swPXFile, strType, strValue, dctParameters, intMinimumValueLength: 0);
        }

        /// <summary>
        /// Append a new header line to the .px file
        /// </summary>
        /// <param name="swPXFile"></param>
        /// <param name="strType"></param>
        /// <param name="strValue"></param>
        /// <param name="dctParameters"></param>
        /// <param name="intMinimumValueLength"></param>
        /// <remarks></remarks>
        private void WritePXHeader(StreamWriter swPXFile, string strType, string strValue, IReadOnlyDictionary<string, string> dctParameters, int intMinimumValueLength)
        {
            string strValueOverride = string.Empty;

            if (dctParameters.TryGetValue(strType, out strValueOverride))
            {
                strValue = strValueOverride;
            }

            if (intMinimumValueLength > 0)
            {
                if (string.IsNullOrEmpty(strValue))
                {
                    strValue = "**** Value must be at least " + intMinimumValueLength + " characters long **** ";
                }

                while (strValue.Length < intMinimumValueLength)
                {
                    strValue += "__";
                }
            }

            WritePXLine(swPXFile, new List<string>
            {
                "MTD",
                strType,
                strValue
            });
        }

        private void WritePXInstruments(StreamWriter swPXFile)
        {
            foreach (var kvInstrumentGroup in mInstrumentGroupsStored)
            {
                var accession = string.Empty;
                var description = string.Empty;

                GetInstrumentAccession(kvInstrumentGroup.Key, out accession, out description);

                if (kvInstrumentGroup.Value.Contains("TSQ_2") && kvInstrumentGroup.Value.Count == 1)
                {
                    // TSQ_1 is a TSQ Quantum Ultra
                    accession = "MS:1000751";
                    description = "TSQ Quantum Ultra";
                }

                var strInstrumentCV = GetInstrumentCv(accession, description);
                WritePXHeader(swPXFile, "instrument", strInstrumentCV);
            }
        }

        private void WritePXLine(TextWriter swPXFile, IReadOnlyCollection<string> lstItems)
        {
            if (lstItems.Count > 0)
            {
                swPXFile.WriteLine(string.Join("\t", lstItems));
            }
        }

        private void WritePXMods(StreamWriter swPXFile)
        {
            if (mModificationsUsed.Count == 0)
            {
                WritePXHeader(swPXFile, "modification", GetCVString("PRIDE", "PRIDE:0000398", "No PTMs are included in the dataset", ""));
            }
            else
            {
                // Write out each modification, for example, for Unimod:
                //   modification	[UNIMOD,UNIMOD:35,Oxidation,]
                // Or for PSI-mod
                //   modification	[MOD,MOD:00394,acetylated residue,]

                foreach (var item in mModificationsUsed)
                {
                    WritePXHeader(swPXFile, "modification", GetCVString(item.Value));
                }
            }
        }

        private void WriteUserParam(XmlTextWriter objXmlWriter, string Name, string Value)
        {
            objXmlWriter.WriteStartElement("userParam");
            objXmlWriter.WriteAttributeString("name", Name);
            objXmlWriter.WriteAttributeString("value", Value);
            objXmlWriter.WriteEndElement();
        }

        private void WriteCVParam(XmlTextWriter objXmlWriter, string CVLabel, string Accession, string Name, string Value)
        {
            objXmlWriter.WriteStartElement("cvParam");
            objXmlWriter.WriteAttributeString("cvLabel", CVLabel);
            objXmlWriter.WriteAttributeString("accession", Accession);
            objXmlWriter.WriteAttributeString("name", Name);
            objXmlWriter.WriteAttributeString("value", Value);
            objXmlWriter.WriteEndElement();
        }

        private bool WriteXMLInstrumentInfo(XmlTextWriter oWriter, string strInstrumentGroup)
        {
            bool blnInstrumentDetailsAutoDefined = false;

            bool blnIsLCQ = false;
            bool blnIsLTQ = false;

            switch (strInstrumentGroup)
            {
                case "Orbitrap":
                case "VelosOrbi":
                case "QExactive":
                    blnInstrumentDetailsAutoDefined = true;

                    WriteXMLInstrumentInfoESI(oWriter, "positive");

                    oWriter.WriteStartElement("analyzerList");
                    oWriter.WriteAttributeString("count", "2");

                    WriteXMLInstrumentInfoAnalyzer(oWriter, "MS", "MS:1000083", "radial ejection linear ion trap");
                    WriteXMLInstrumentInfoAnalyzer(oWriter, "MS", "MS:1000484", "orbitrap");

                    oWriter.WriteEndElement();   // analyzerList

                    WriteXMLInstrumentInfoDetector(oWriter, "MS", "MS:1000624", "inductive detector");

                    break;
                case "LCQ":
                    blnIsLCQ = true;
                    break;
                case "LTQ":
                case "LTQ-ETD":
                case "LTQ-Prep":
                case "VelosPro":
                    blnIsLTQ = true;
                    break;
                case "LTQ-FT":
                    blnInstrumentDetailsAutoDefined = true;

                    WriteXMLInstrumentInfoESI(oWriter, "positive");

                    oWriter.WriteStartElement("analyzerList");
                    oWriter.WriteAttributeString("count", "2");

                    WriteXMLInstrumentInfoAnalyzer(oWriter, "MS", "MS:1000083", "radial ejection linear ion trap");
                    WriteXMLInstrumentInfoAnalyzer(oWriter, "MS", "MS:1000079", "fourier transform ion cyclotron resonance mass spectrometer");

                    oWriter.WriteEndElement();   // analyzerList

                    WriteXMLInstrumentInfoDetector(oWriter, "MS", "MS:1000624", "inductive detector");

                    break;
                case "Exactive":
                    blnInstrumentDetailsAutoDefined = true;

                    WriteXMLInstrumentInfoESI(oWriter, "positive");

                    oWriter.WriteStartElement("analyzerList");
                    oWriter.WriteAttributeString("count", "1");

                    WriteXMLInstrumentInfoAnalyzer(oWriter, "MS", "MS:1000484", "orbitrap");

                    oWriter.WriteEndElement();   // analyzerList

                    WriteXMLInstrumentInfoDetector(oWriter, "MS", "MS:1000624", "inductive detector");

                    break;
                default:
                    if (strInstrumentGroup.StartsWith("LTQ"))
                    {
                        blnIsLTQ = true;
                    }
                    else if (strInstrumentGroup.StartsWith("LCQ"))
                    {
                        blnIsLCQ = true;
                    }
                    break;
            }

            if (blnIsLTQ | blnIsLCQ)
            {
                blnInstrumentDetailsAutoDefined = true;

                WriteXMLInstrumentInfoESI(oWriter, "positive");

                oWriter.WriteStartElement("analyzerList");
                oWriter.WriteAttributeString("count", "1");

                if (blnIsLCQ)
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

            return blnInstrumentDetailsAutoDefined;
        }

        private void WriteXMLInstrumentInfoAnalyzer(XmlTextWriter oWriter, string strNamespace, string strAccession, string strDescription)
        {
            oWriter.WriteStartElement("analyzer");
            WriteCVParam(oWriter, strNamespace, strAccession, strDescription, string.Empty);
            oWriter.WriteEndElement();
        }

        private void WriteXMLInstrumentInfoDetector(XmlTextWriter oWriter, string strNamespace, string strAccession, string strDescription)
        {
            oWriter.WriteStartElement("detector");
            WriteCVParam(oWriter, strNamespace, strAccession, strDescription, string.Empty);
            oWriter.WriteEndElement();
        }

        private void WriteXMLInstrumentInfoESI(XmlTextWriter oWriter, string strPolarity)
        {
            if (string.IsNullOrEmpty(strPolarity))
                strPolarity = "positive";

            oWriter.WriteStartElement("source");
            WriteCVParam(oWriter, "MS", "MS:1000073", "electrospray ionization", string.Empty);
            WriteCVParam(oWriter, "MS", "MS:1000037", "polarity", strPolarity);
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

        private void mDTAtoMGF_ErrorEvent(string strMessage)
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
                foreach (var kvUnzippedFile in m_MyEMSLUtilities.MostRecentUnzippedFiles)
                {
                    AddToListIfNew(mPreviousDatasetFilesToDelete, kvUnzippedFile.Value);
                }
            }

            AddToListIfNew(mPreviousDatasetFilesToDelete, Path.Combine(e.DownloadFolderPath, e.ArchivedFile.Filename));
        }

        #endregion
    }
}
