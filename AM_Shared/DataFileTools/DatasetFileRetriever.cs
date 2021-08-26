using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using PRISMDatabaseUtils;

namespace AnalysisManagerBase.DataFileTools
{
    /// <summary>
    /// Utilities for copying dataset files locally
    /// </summary>
    public class DatasetFileRetriever : PRISM.EventNotifier
    {
        /// <summary>
        /// Error message
        /// </summary>
        /// <remarks>
        /// This is cleared when <see cref="RetrieveInstrumentFilesForJobDatasets"/>
        /// or <see cref="RetrieveDataPackageDatasetFiles"/> is called
        /// </remarks>
        public string ErrorMessage { get; private set; }

        private readonly AnalysisResources mResourceClass;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="resourcer"></param>
        public DatasetFileRetriever(AnalysisResources resourcer)
        {
            mResourceClass = resourcer;
            ErrorMessage = string.Empty;
        }

        private CloseOutType GetDatasetFile(string rawDataTypeName)
        {
            if (mResourceClass.FileSearchTool.RetrieveSpectra(rawDataTypeName))
            {
                // Raw file
                mResourceClass.AddResultFileExtensionToSkip(AnalysisResources.DOT_RAW_EXTENSION);
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            mResourceClass.LogDebug("DatasetFileRetriever.GetDatasetFile: Error occurred retrieving spectra.");
            return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
        }

        private string GetDatasetFileOrDirectoryName(AnalysisResources.RawDataTypeConstants rawDataType, out bool isDirectory)
        {
            switch (rawDataType)
            {
                case AnalysisResources.RawDataTypeConstants.ThermoRawFile:
                    isDirectory = false;
                    return mResourceClass.DatasetName + AnalysisResources.DOT_RAW_EXTENSION;

                case AnalysisResources.RawDataTypeConstants.AgilentDFolder:
                case AnalysisResources.RawDataTypeConstants.BrukerTOFBaf:
                case AnalysisResources.RawDataTypeConstants.BrukerFTFolder:
                    isDirectory = true;
                    return mResourceClass.DatasetName + AnalysisResources.DOT_D_EXTENSION;

                case AnalysisResources.RawDataTypeConstants.mzXML:
                    isDirectory = false;
                    return mResourceClass.DatasetName + AnalysisResources.DOT_MZXML_EXTENSION;

                case AnalysisResources.RawDataTypeConstants.mzML:
                    isDirectory = false;
                    return mResourceClass.DatasetName + AnalysisResources.DOT_MZML_EXTENSION;

                default:
                    throw new ArgumentOutOfRangeException(nameof(rawDataType), "Unsupported raw data type: " + rawDataType);
            }
        }

        /// <summary>
        /// Retrieve instrument files for either the current job (if dataPackageID is 0)
        /// or for the jobs associated with a data package
        /// </summary>
        /// <param name="dataPackageID">0 to use the current job and dataset, non-zero to use jobs in a data package</param>
        /// <param name="usingMzML">True if this job's settings file indicates to use .mzML files instead of the original instrument file</param>
        /// <param name="progressPercentAtFinish">Final percent complete value to use for computing incremental progress</param>
        /// <param name="dataPackageInfo">Output: new instance of DataPackageInfo, which tracks datasets, experiments, files, etc. associated with this job</param>
        /// <param name="dataPackageDatasets">Output: keys are Dataset ID, values are dataset info</param>
        public CloseOutType RetrieveInstrumentFilesForJobDatasets(
            int dataPackageID,
            bool usingMzML,
            float progressPercentAtFinish,
            out DataPackageInfo dataPackageInfo,
            out Dictionary<int, DataPackageDatasetInfo> dataPackageDatasets)
        {
            ErrorMessage = string.Empty;

            dataPackageInfo = new DataPackageInfo(dataPackageID);
            RegisterEvents(dataPackageInfo);

            try
            {
                var workingDirectory = new DirectoryInfo(mResourceClass.WorkDir);

                // ReSharper disable once ConvertIfStatementToReturnStatement
                if (dataPackageID > 0)
                {
                    return RetrieveDataPackageDatasets(dataPackageInfo, usingMzML, progressPercentAtFinish, out dataPackageDatasets);
                }

                return RetrieveSingleDataset(workingDirectory, dataPackageInfo, out dataPackageDatasets);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in RetrieveInstrumentFilesForJobDatasets", ex);
                dataPackageDatasets = new Dictionary<int, DataPackageDatasetInfo>();
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Determine the dataset files associated with the current data package
        /// </summary>
        /// <param name="dataPackageInfo"></param>
        /// <param name="usingMzML">True if this job's settings file indicates to use .mzML files instead of the original instrument file</param>
        /// <param name="progressPercentAtFinish">Final percent complete value to use for computing incremental progress</param>
        /// <param name="dataPackageDatasets">Output: keys are Dataset ID, values are dataset info</param>
        private CloseOutType RetrieveDataPackageDatasets(
            DataPackageInfo dataPackageInfo,
            bool usingMzML,
            float progressPercentAtFinish,
            out Dictionary<int, DataPackageDatasetInfo> dataPackageDatasets)
        {
            try
            {
                // Keys in dictionary datasetRawFilePaths are dataset name, values are paths to the local file or directory for the dataset</param>

                var filesRetrieved = RetrieveDataPackageDatasetFiles(
                    usingMzML,
                    out dataPackageDatasets, out var datasetRawFilePaths,
                    0, progressPercentAtFinish
                    );

                if (!filesRetrieved)
                {
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                foreach (var dataset in dataPackageDatasets)
                {
                    var datasetID = dataset.Key;
                    var datasetName = dataset.Value.Dataset;

                    var datasetFileName = Path.GetFileName(datasetRawFilePaths[datasetName]);

                    dataPackageInfo.Datasets.Add(datasetID, datasetName);
                    dataPackageInfo.Experiments.Add(datasetID, dataset.Value.Experiment);

                    dataPackageInfo.DatasetFiles.Add(datasetID, datasetFileName);
                    dataPackageInfo.DatasetFileTypes.Add(datasetID, dataset.Value.IsDirectoryBased ? "Directory" : "File");

                    dataPackageInfo.DatasetExperimentGroup.Add(datasetID, dataset.Value.DatasetExperimentGroup);
                    dataPackageInfo.DatasetMaxQuantParamGroup.Add(datasetID, dataset.Value.MaxQuantParamGroup);

                    mResourceClass.AddResultFileToSkip(datasetFileName);
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in RetrieveDataPackageDatasets", ex);
                dataPackageDatasets = new Dictionary<int, DataPackageDatasetInfo>();
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Retrieves the instrument files for the datasets defined for the data package associated with this aggregation job
        /// </summary>
        /// <param name="retrieveMzMLFiles">Set to true to obtain mzML files for the datasets; will return false if a .mzML file cannot be found for any of the datasets</param>
        /// <param name="dataPackageDatasets">Output parameter: Dataset info for the datasets associated with this data package; keys are Dataset ID</param>
        /// <param name="datasetRawFilePaths">Output parameter: Keys in this dictionary are dataset name, values are paths to the local file or directory for the dataset</param>
        /// <param name="progressPercentAtStart">Percent complete value to use for computing incremental progress</param>
        /// <param name="progressPercentAtFinish">Percent complete value to use for computing incremental progress</param>
        /// <returns>True if success, false if an error</returns>
        public bool RetrieveDataPackageDatasetFiles(
            bool retrieveMzMLFiles,
            out Dictionary<int, DataPackageDatasetInfo> dataPackageDatasets,
            out Dictionary<string, string> datasetRawFilePaths,
            float progressPercentAtStart,
            float progressPercentAtFinish)
        {
            ErrorMessage = string.Empty;

            // Gigasax.DMS_Pipeline
            var brokerDbConnectionString = mResourceClass.MgrParams.GetParam("BrokerConnectionString");

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(brokerDbConnectionString, mResourceClass.MgrParams.ManagerName);

            var dataPackageID = mResourceClass.JobParams.GetJobParameter("DataPackageID", -1);

            var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: mResourceClass.TraceMode);
            RegisterEvents(dbTools);

            var dataPackageFileHandler = new DataPackageFileHandler(dbTools, dataPackageID, mResourceClass);
            RegisterEvents(dataPackageFileHandler);

            var success = dataPackageFileHandler.RetrieveDataPackageDatasetFiles(
                retrieveMzMLFiles, out dataPackageDatasets, out datasetRawFilePaths,
                progressPercentAtStart, progressPercentAtFinish);

            if (!success)
            {
                ErrorMessage = dataPackageFileHandler.ErrorMessage;
            }

            return success;
        }

        /// <summary>
        /// Retrieve the dataset file for the current dataset
        /// </summary>
        /// <param name="workingDirectory"></param>
        /// <param name="dataPackageInfo"></param>
        /// <param name="dataPackageDatasets">Output: keys are Dataset ID, values are dataset info</param>
        private CloseOutType RetrieveSingleDataset(
            FileSystemInfo workingDirectory,
            DataPackageInfo dataPackageInfo,
            out Dictionary<int, DataPackageDatasetInfo> dataPackageDatasets)
        {
            var currentTask = "Initializing";
            dataPackageDatasets = new Dictionary<int, DataPackageDatasetInfo>();

            try
            {
                var experiment = mResourceClass.JobParams.GetJobParameter("Experiment", string.Empty);

                var datasetID = mResourceClass.JobParams.GetJobParameter("DatasetID", 0);

                dataPackageInfo.Datasets.Add(datasetID, mResourceClass.DatasetName);
                dataPackageInfo.Experiments.Add(datasetID, experiment);

                currentTask = "Lookup dataset metadata";

                var rawDataTypeName = mResourceClass.JobParams.GetParam("RawDataType");
                var rawDataType = AnalysisResources.GetRawDataType(rawDataTypeName);

                var instrumentName = mResourceClass.JobParams.GetParam("Instrument");
                var instrumentGroup = mResourceClass.JobParams.GetParam("InstrumentGroup");
                var experimentName = mResourceClass.JobParams.GetParam("Experiment");

                var datasetStoragePath = mResourceClass.JobParams.GetParam("DatasetStoragePath");
                var datasetDirectoryName = mResourceClass.JobParams.GetParam("DatasetFolderName");
                var datasetArchivePath = mResourceClass.JobParams.GetParam("DatasetArchivePath");

                var dataPackageDatasetInfo = new DataPackageDatasetInfo(mResourceClass.DatasetName, datasetID)
                {
                    Instrument = instrumentName,
                    InstrumentGroup = instrumentGroup,
                    IsDirectoryBased = false,
                    Experiment = experimentName,
                    DatasetDirectoryPath = Path.Combine(datasetStoragePath, datasetDirectoryName),
                    DatasetArchivePath = datasetArchivePath,
                    RawDataType = rawDataTypeName
                };

                dataPackageDatasets.Add(datasetID, dataPackageDatasetInfo);

                var usingMzML = mResourceClass.JobParams.GetJobParameter("CreateMzMLFiles", false);

                if (usingMzML)
                {
                    currentTask = "GetMzMLFile";

                    var mzMLResultCode = mResourceClass.GetMzMLFile();

                    if (mzMLResultCode != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        return mzMLResultCode;
                    }

                    dataPackageInfo.DatasetFiles.Add(datasetID, mResourceClass.DatasetName + AnalysisResources.DOT_MZML_EXTENSION);
                    dataPackageInfo.DatasetFileTypes.Add(datasetID, "File");

                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                currentTask = "Get the primary dataset file";
                var retrievalAttempts = 0;

                while (retrievalAttempts < 2)
                {
                    retrievalAttempts++;
                    switch (rawDataTypeName.ToLower())
                    {
                        case AnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES:
                        case AnalysisResources.RAW_DATA_TYPE_DOT_D_FOLDERS:
                        case AnalysisResources.RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER:
                        case AnalysisResources.RAW_DATA_TYPE_BRUKER_FT_FOLDER:
                            currentTask = string.Format("Retrieve spectra: {0}; instrument: {1}", rawDataTypeName, instrumentName);
                            var datasetResult = GetDatasetFile(rawDataTypeName);
                            if (datasetResult == CloseOutType.CLOSEOUT_FILE_NOT_FOUND)
                                return datasetResult;

                            var datasetFileOrDirectoryName = GetDatasetFileOrDirectoryName(rawDataType, out var isDirectory);

                            dataPackageInfo.DatasetFiles.Add(datasetID, datasetFileOrDirectoryName);
                            dataPackageInfo.DatasetFileTypes.Add(datasetID, isDirectory ? "Directory" : "File");

                            dataPackageDatasetInfo.IsDirectoryBased = isDirectory;
                            break;

                        default:
                            ErrorMessage = "Dataset type " + rawDataTypeName + " is not supported";

                            OnDebugEvent(
                                "DatasetFileRetriever.GetResources: " + ErrorMessage + "; must be " +
                                AnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES + ", " +
                                AnalysisResources.RAW_DATA_TYPE_DOT_D_FOLDERS + ", " +
                                AnalysisResources.RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER + ", " +
                                AnalysisResources.RAW_DATA_TYPE_BRUKER_FT_FOLDER);

                            return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }

                    if (mResourceClass.MyEMSLUtils.FilesToDownload.Count == 0)
                    {
                        break;
                    }

                    currentTask = "ProcessMyEMSLDownloadQueue";
                    if (mResourceClass.MyEMSLUtils.ProcessMyEMSLDownloadQueue(workingDirectory.FullName, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
                    {
                        break;
                    }

                    // Look for this file on the Samba share
                    mResourceClass.DisableMyEMSLSearch();
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in RetrieveSingleDataset (CurrentTask = " + currentTask + ")", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }
    }
}
