using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml.Linq;
using AnalysisManagerBase;

namespace AnalysisManagerMaxQuantPlugIn
{
    /// <summary>
    /// Retrieve resources for the MaxQuant plugin
    /// </summary>
    public class AnalysisResourcesMaxQuant : AnalysisResources
    {
        // Ignore Spelling: MaxQuant, Parm, Maxq

        private const string MAXQUANT_PEAK_STEP_TOOL = "MaxqPeak";

        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, MyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(Global.eAnalysisResourceOptions.OrgDbRequired, true);
        }

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            var currentTask = "Initializing";

            try
            {
                // Retrieve shared resources, including the JobParameters file from the previous job step
                var result = GetSharedResources();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                var workingDirectory = new DirectoryInfo(mWorkDir);

                var transferDirectoryPath = GetTransferFolderPathForJobStep(useInputDirectory: true);

                var paramFileName = mJobParams.GetParam(JOB_PARAM_PARAMETER_FILE);
                currentTask = "RetrieveParamFile " + paramFileName;

                // Retrieve param file
                if (!FileSearch.RetrieveFile(paramFileName, mJobParams.GetParam("ParmFileStoragePath")))
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;

                var previousStepParamFileFound = GetExistingToolParametersFile(
                    workingDirectory, transferDirectoryPath, paramFileName, out var previousJobStepParameterFilePath);

                if (previousStepParamFileFound)
                {
                    var skipStepToolPrevJobStep = CheckSkipMaxQuant(
                        workingDirectory, previousJobStepParameterFilePath, out var abortProcessingPrevJobStep, out var skipReason);

                    if (abortProcessingPrevJobStep)
                    {
                        return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                    }

                    if (skipStepToolPrevJobStep)
                    {
                        // This message should have already been logged
                        EvalMessage = skipReason;
                        return CloseOutType.CLOSEOUT_SKIPPED_MAXQUANT;
                    }
                }

                // Also examine the original parameter file, in case it has numeric values defined for startStepID
                var skipStepTool = CheckSkipMaxQuant(workingDirectory, paramFileName, out var abortProcessing, out var skipReason2);

                if (abortProcessing)
                {
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }

                if (skipStepTool)
                {
                    // This message should have already been logged
                    EvalMessage = skipReason2;
                    return CloseOutType.CLOSEOUT_SKIPPED_MAXQUANT;
                }

                // Retrieve Fasta file
                var orgDbDirectoryPath = mMgrParams.GetParam(MGR_PARAM_ORG_DB_DIR);

                currentTask = "RetrieveOrgDB to " + orgDbDirectoryPath;
                if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                    return resultCode;

                var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);

                var dataPackageInfo = new DataPackageInfo(dataPackageID);

                CloseOutType datasetCopyResult;

                if (dataPackageID > 0)
                {
                    datasetCopyResult = RetrieveDataPackageDatasets(dataPackageInfo);
                }
                else
                {
                    datasetCopyResult = RetrieveSingleDataset(workingDirectory, dataPackageInfo);
                }

                if (datasetCopyResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return datasetCopyResult;
                }

                // Store information about the datasets in several packed job parameters
                dataPackageInfo.StorePackedDictionaries(this);

                // Find files in the transfer directory that should be copied locally
                // For zip files, copy and unzip each one
                var fileCopyResult = RetrieveTransferDirectoryFiles(workingDirectory, transferDirectoryPath);

                if (fileCopyResult != CloseOutType.CLOSEOUT_SUCCESS)
                    return fileCopyResult;
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Exception in GetResources (CurrentTask = " + currentTask + ")", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType RetrieveTransferDirectoryFiles(FileSystemInfo workingDirectory, string transferDirectoryPath)
        {
            var targetFilePath = "??";

            try
            {
                if (string.IsNullOrEmpty(transferDirectoryPath))
                {
                    // Transfer directory parameter is empty; nothing to retrieve
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                var zipTools = new DotNetZipTools(mDebugLevel, workingDirectory.FullName);
                RegisterEvents(zipTools);

                var transferDirectory = new DirectoryInfo(transferDirectoryPath);

                foreach (var item in transferDirectory.GetFiles("*", SearchOption.AllDirectories))
                {
                    var relativeFilePath = item.FullName.Substring(transferDirectory.FullName.Length);

                    targetFilePath = Path.Combine(workingDirectory.FullName, relativeFilePath);
                    var targetFile = new FileInfo(targetFilePath);

                    if (targetFile.Exists)
                        continue;

                    var copySuccess = mFileTools.CopyFileUsingLocks(item, targetFile.FullName, true);
                    if (!copySuccess)
                    {
                        LogError(string.Format("Error copying file {0} to {1}", item.FullName, targetFile.FullName));
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    if (!targetFile.Extension.Equals(".zip", StringComparison.OrdinalIgnoreCase))
                        continue;

                    var targetDirectory = Path.Combine(workingDirectory.FullName, Path.GetFileNameWithoutExtension(relativeFilePath));

                    // Unzip the file
                    var unzipSuccess = zipTools.UnzipFile(targetFile.FullName, targetDirectory);
                    if (!unzipSuccess)
                    {
                        LogError(string.Format("Error unzipping file {0} to {1}", targetFile.FullName, targetDirectory));
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    // Delete the zip file
                    targetFile.Delete();
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Exception in RetrieveTransferDirectoryFiles for file " + targetFilePath, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private bool CheckSkipMaxQuant(FileSystemInfo workingDirectory, string maxQuantParameterFileName, out bool abortProcessing, out string skipReason)
        {
            abortProcessing = false;
            skipReason = string.Empty;

            try
            {
                var sourceFile = new FileInfo(Path.Combine(workingDirectory.FullName, maxQuantParameterFileName));

                using var reader = new StreamReader(new FileStream(sourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                // Note that XDocument supersedes XmlDocument and XPathDocument
                // XDocument can often be easier to use since XDocument is LINQ-based

                var doc = XDocument.Parse(reader.ReadToEnd());

                var dmsStepNodes = doc.Elements("MaxQuantParams").Elements("dmsSteps").Elements("step").ToList();

                if (dmsStepNodes.Count == 0)
                {
                    // Step tool MaxqPeak will run all of the MaxQuant steps
                    // Skip processing for step tools MaxqS1, MaxqS2, and MaxqS3

                    if (!StepToolName.Equals(MAXQUANT_PEAK_STEP_TOOL, StringComparison.OrdinalIgnoreCase))
                    {
                        skipReason = string.Format(
                            "Skipping '{0}' since step tool '{1}' should have already run MaxQuant to completion",
                            StepToolName, MAXQUANT_PEAK_STEP_TOOL);

                        LogMessage(skipReason);
                        return true;
                    }
                }

                var dmsSteps = new Dictionary<int, DmsStepInfo>();

                // Get the DMS step info
                foreach (var item in dmsStepNodes)
                {
                    if (!AnalysisToolRunnerMaxQuant.GetDmsStepDetails(item, out var dmsStepInfo, out var errorMessage))
                    {
                        LogError(errorMessage);
                        abortProcessing = true;
                        return false;
                    }

                    dmsSteps.Add(dmsStepInfo.ID, dmsStepInfo);
                }

                var countWithValue = dmsSteps.Count(item => item.Value.StartStepID.HasValue);

                if (countWithValue == 0)
                {
                    // None of the steps has an integer defined for StartStepID
                    // MaxQuant has thus not been run yet
                    return false;
                }

                if (countWithValue < dmsSteps.Count)
                {
                    LogError("DMS steps in the MaxQuant parameter file have a mix of integer startStepID's and text=based startStepID's; " +
                             "either all should have 'auto' or all should have integers");
                    abortProcessing = true;
                    return false;
                }

                // All of the steps have a step ID defined
                // Examine the StartStepID for the step that matches this step tool
                foreach (var dmsStep in dmsSteps.Where(dmsStep => dmsStep.Value.Tool.Equals(StepToolName)))
                {
                    if (dmsStep.Value.StartStepID >= 0)
                        return false;

                    // Skip this step tool
                    LogMessage(string.Format(
                        "Skipping step tool {0} since the StartStepID value in the MaxQuant parameter file is negative",
                        StepToolName));

                    return true;
                }

                // Match not found
                LogMessage(string.Format(
                    "Skipping step tool {0} since none of the tool names in the dmsSteps section of the MaxQuant parameter file matched this step tool",
                    StepToolName));

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception in CheckSkipMaxQuant", ex);
                abortProcessing = true;
                return false;
            }
        }

        private CloseOutType GetDatasetFile(string rawDataTypeName)
        {
            if (FileSearch.RetrieveSpectra(rawDataTypeName))
            {
                // Raw file
                mJobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION);
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            LogDebug("AnalysisResourcesMaxQuant.GetDatasetFile: Error occurred retrieving spectra.");
            return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
        }

        private string GetDatasetFileOrDirectoryName(eRawDataTypeConstants rawDataType, out bool isDirectory)
        {
            switch (rawDataType)
            {
                case eRawDataTypeConstants.ThermoRawFile:
                    isDirectory = false;
                    return DatasetName + DOT_RAW_EXTENSION;

                case eRawDataTypeConstants.AgilentDFolder:
                case eRawDataTypeConstants.BrukerTOFBaf:
                case eRawDataTypeConstants.BrukerFTFolder:
                    isDirectory = true;
                    return DatasetName + DOT_D_EXTENSION;

                case eRawDataTypeConstants.mzXML:
                    isDirectory = false;
                    return DatasetName + DOT_MZXML_EXTENSION;

                case eRawDataTypeConstants.mzML:
                    isDirectory = false;
                    return DatasetName + DOT_MZML_EXTENSION;

                default:
                    throw new ArgumentOutOfRangeException(nameof(rawDataType), "Unsupported raw data type: " + rawDataType);
            }
        }

        /// <summary>
        /// Look for a step tool parameter file from the previous job step
        /// If found, copy to the working directory, naming it ToolParameters_JobNum_PreviousStep.xml
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        private bool GetExistingToolParametersFile(
            FileSystemInfo workingDirectory,
            string transferDirectoryPath,
            string paramFileName,
            out string previousJobStepParameterFilePath)
        {
            previousJobStepParameterFilePath = string.Empty;
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

                if (string.IsNullOrEmpty(transferDirectoryPath))
                {
                    // Transfer directory parameter is empty; nothing to retrieve
                    return true;
                }

                var sourceFile = new FileInfo(Path.Combine(transferDirectoryPath, paramFileName));

                if (!sourceFile.Exists)
                {
                    // File not found; nothing to copy
                    return true;
                }

                // Copy the file, renaming to avoid a naming collision
                var destinationFilePath = Path.Combine(workingDirectory.FullName, Path.GetFileNameWithoutExtension(sourceFile.Name) + "_PreviousStep.xml");
                if (mFileCopyUtilities.CopyFileWithRetry(sourceFile.FullName, destinationFilePath, overwrite: true, maxCopyAttempts: 3))
                {
                    if (mDebugLevel > 3)
                    {
                        LogDebugMessage("GetExistingToolParametersFile, File copied:  " + sourceFile.FullName);
                    }

                    previousJobStepParameterFilePath = destinationFilePath;
                    return true;
                }

                LogError("Error in GetExistingToolParametersFile copying file " + sourceFile.FullName);
                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception in GetExistingToolParametersFile", ex);
                return false;
            }
        }

        private CloseOutType RetrieveDataPackageDatasets(DataPackageInfo dataPackageInfo)
        {
            try
            {
                var usingMzML = mJobParams.GetJobParameter("CreateMzMLFiles", false);

                // Keys in dictionary dataPackageDatasets are Dataset ID, values are dataset info
                // Keys in dictionary datasetRawFilePaths are dataset name, values are paths to the local file or directory for the dataset</param>

                var filesRetrieved = RetrieveDataPackageDatasetFiles(
                    usingMzML,
                    out var dataPackageDatasets, out var datasetRawFilePaths,
                    0,
                    AnalysisToolRunnerMaxQuant.PROGRESS_PCT_TOOL_RUNNER_STARTING);

                if (!filesRetrieved)
                {
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                foreach (var dataset in dataPackageDatasets)
                {
                    var datasetID = dataset.Key;
                    var datasetName = dataset.Value.Dataset;

                    var localDatasetPath = datasetRawFilePaths[datasetName];

                    dataPackageInfo.Datasets.Add(datasetID, datasetName);
                    dataPackageInfo.Experiments.Add(datasetID, dataset.Value.Experiment);

                    dataPackageInfo.DatasetFiles.Add(datasetID, Path.GetFileName(localDatasetPath));
                    dataPackageInfo.DatasetFileTypes.Add(datasetID, dataset.Value.IsDirectoryBased ? "Directory" : "File");
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Exception in RetrieveDataPackageDatasets", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType RetrieveSingleDataset(FileSystemInfo workingDirectory, DataPackageInfo dataPackageInfo)
        {
            var currentTask = "Initializing";

            try
            {
                var experiment = mJobParams.GetJobParameter("Experiment", string.Empty);

                var datasetID = mJobParams.GetJobParameter("DatasetID", 0);

                dataPackageInfo.Datasets.Add(datasetID, DatasetName);
                dataPackageInfo.Experiments.Add(datasetID, experiment);

                var usingMzML = mJobParams.GetJobParameter("CreateMzMLFiles", false);

                if (usingMzML)
                {
                    currentTask = "GetMzMLFile";

                    var mzMLResultCode = GetMzMLFile();

                    if (mzMLResultCode != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        return mzMLResultCode;
                    }

                    dataPackageInfo.DatasetFiles.Add(datasetID, DatasetName + DOT_MZML_EXTENSION);
                    dataPackageInfo.DatasetFileTypes.Add(datasetID, "File");
                }
                else
                {
                    // Get the primary dataset file
                    currentTask = "Determine RawDataType";

                    var rawDataTypeName = mJobParams.GetParam("RawDataType");
                    var rawDataType = GetRawDataType(rawDataTypeName);

                    var instrumentName = mJobParams.GetParam("Instrument");

                    var retrievalAttempts = 0;

                    while (retrievalAttempts < 2)
                    {
                        retrievalAttempts++;
                        switch (rawDataTypeName.ToLower())
                        {
                            case RAW_DATA_TYPE_DOT_RAW_FILES:
                            case RAW_DATA_TYPE_DOT_D_FOLDERS:
                            case RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER:
                            case RAW_DATA_TYPE_BRUKER_FT_FOLDER:
                                currentTask = string.Format("Retrieve spectra: {0}; instrument: {1}", rawDataTypeName, instrumentName);
                                var datasetResult = GetDatasetFile(rawDataTypeName);
                                if (datasetResult == CloseOutType.CLOSEOUT_FILE_NOT_FOUND)
                                    return datasetResult;

                                var datasetFileOrDirectoryName = GetDatasetFileOrDirectoryName(rawDataType, out var isDirectory);

                                dataPackageInfo.DatasetFiles.Add(datasetID, datasetFileOrDirectoryName);
                                dataPackageInfo.DatasetFileTypes.Add(datasetID, isDirectory ? "Directory" : "File");

                                break;

                            default:
                                mMessage = "Dataset type " + rawDataTypeName + " is not supported";
                                LogDebug(
                                    "AnalysisResourcesMaxQuant.GetResources: " + mMessage + "; must be " +
                                    RAW_DATA_TYPE_DOT_RAW_FILES + ", " +
                                    RAW_DATA_TYPE_DOT_D_FOLDERS + ", " +
                                    RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER + ", " +
                                    RAW_DATA_TYPE_BRUKER_FT_FOLDER);

                                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                        }

                        if (mMyEMSLUtilities.FilesToDownload.Count == 0)
                        {
                            break;
                        }

                        currentTask = "ProcessMyEMSLDownloadQueue";
                        if (mMyEMSLUtilities.ProcessMyEMSLDownloadQueue(workingDirectory.FullName, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
                        {
                            break;
                        }

                        // Look for this file on the Samba share
                        DisableMyEMSLSearch();
                    }
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Exception in RetrieveSingleDataset (CurrentTask = " + currentTask + ")", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }
    }
}
