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
        // Ignore Spelling: resourcer

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
        /// <param name="resourcer">Resourcer instance</param>
        public DatasetFileRetriever(AnalysisResources resourcer)
        {
            mResourceClass = resourcer;
            ErrorMessage = string.Empty;
        }

        /// <summary>
        /// Look for a .D directory for this dataset
        /// </summary>
        /// <returns>True if found, otherwise empty string</returns>
        private bool DatasetHasAgilentDotD(string datasetName)
        {
            mResourceClass.FindValidDirectory(
                datasetName, string.Empty, datasetName + ".d", 2,
                false,
                false,
                out var validDirectoryFound,
                false,
                out _);

            if (validDirectoryFound)
            {
                OnDebugEvent("Found .d directory for " + datasetName);
                return true;
            }

            // ReSharper disable once StringLiteralTypo
            OnStatusEvent("Did not find a .d directory for " + datasetName + "; will process the dataset's .UIMF file");
            return false;
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
                case AnalysisResources.RawDataTypeConstants.BrukerFTFolder:
                case AnalysisResources.RawDataTypeConstants.BrukerTOFBaf:
                case AnalysisResources.RawDataTypeConstants.BrukerTOFTdf:
                    isDirectory = true;
                    return mResourceClass.DatasetName + AnalysisResources.DOT_D_EXTENSION;

                case AnalysisResources.RawDataTypeConstants.mzXML:
                    isDirectory = false;
                    return mResourceClass.DatasetName + AnalysisResources.DOT_MZXML_EXTENSION;

                case AnalysisResources.RawDataTypeConstants.mzML:
                    isDirectory = false;
                    return mResourceClass.DatasetName + AnalysisResources.DOT_MZML_EXTENSION;

                case AnalysisResources.RawDataTypeConstants.UIMF:
                    isDirectory = false;
                    return mResourceClass.DatasetName + AnalysisResources.DOT_UIMF_EXTENSION;

                default:
                    throw new ArgumentOutOfRangeException(nameof(rawDataType), "Unsupported raw data type: " + rawDataType);
            }
        }

        /// <summary>
        /// Retrieve instrument files for either the current job (if dataPackageID is 0)
        /// or for the jobs associated with a data package
        /// </summary>
        /// <param name="dataPackageID">0 to use the current job and dataset, positive number to use datasets in a data package</param>
        /// <param name="retrieveMsXmlFiles">
        /// <para>
        /// True if this job's settings file indicates to use .mzML (or .mzXML) files instead of the original instrument file
        /// </para>
        /// <para>
        /// In addition, classes AnalysisResourcesPepProtProphet and AnalysisResourcesMSFragger set this to true, regardless of the settings file
        /// </para>
        /// </param>
        /// <param name="progressPercentAtFinish">Final percent complete value to use for computing incremental progress</param>
        /// <param name="skipDatasetsWithExistingMzML">
        /// When true, for each dataset in a data package, if an existing .mzML file can be found,
        /// do not retrieve the .raw file and do not add to dataPackageDatasets
        /// </param>
        /// <param name="dataPackageInfo">Output: new instance of DataPackageInfo, which tracks datasets, experiments, files, etc. associated with this job</param>
        /// <param name="dataPackageDatasets">Output: keys are Dataset ID, values are dataset info</param>
        public CloseOutType RetrieveInstrumentFilesForJobDatasets(
            int dataPackageID,
            bool retrieveMsXmlFiles,
            float progressPercentAtFinish,
            bool skipDatasetsWithExistingMzML,
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
                    return RetrieveDataPackageDatasets(dataPackageInfo, retrieveMsXmlFiles, progressPercentAtFinish, skipDatasetsWithExistingMzML, out dataPackageDatasets);
                }

                return RetrieveSingleDataset(workingDirectory, dataPackageInfo, retrieveMsXmlFiles, out dataPackageDatasets);
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
        /// <param name="dataPackageInfo">Data package info; dictionaries in this object will be populated by this method (using data obtained from RetrieveDataPackageDatasetFiles)</param>
        /// <param name="retrieveMsXmlFiles">
        /// <para>
        /// True if this job's settings file indicates to use .mzML (or .mzXML) files instead of the original instrument file
        /// </para>
        /// <para>
        /// In addition, classes AnalysisResourcesPepProtProphet and AnalysisResourcesMSFragger set this to true, regardless of the settings file
        /// </para>
        /// </param>
        /// <param name="progressPercentAtFinish">Final percent complete value to use for computing incremental progress</param>
        /// <param name="skipDatasetsWithExistingMzML">
        /// When true, for each dataset in the data package, if an existing .mzML file can be found,
        /// do not retrieve the .raw file and do not add to dataPackageInfo.DatasetFiles, dataPackageInfo.DatasetFileTypes, or dataPackageInfo.DatasetRawDataTypeNames
        /// </param>
        /// <param name="dataPackageDatasets">Output: keys are Dataset ID, values are dataset info</param>
        private CloseOutType RetrieveDataPackageDatasets(
            DataPackageInfo dataPackageInfo,
            bool retrieveMsXmlFiles,
            float progressPercentAtFinish,
            bool skipDatasetsWithExistingMzML,
            out Dictionary<int, DataPackageDatasetInfo> dataPackageDatasets)
        {
            try
            {
                // Keys in dictionary datasetRawFilePaths are dataset name, values are paths to the local file or directory for the dataset

                var filesRetrieved = RetrieveDataPackageDatasetFiles(
                    retrieveMsXmlFiles,
                    out dataPackageDatasets,
                    out var datasetRawFilePaths,
                    0,
                    progressPercentAtFinish,
                    skipDatasetsWithExistingMzML
                    );

                if (!filesRetrieved)
                {
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                foreach (var dataset in dataPackageDatasets)
                {
                    var datasetID = dataset.Key;
                    var datasetName = dataset.Value.Dataset;

                    dataPackageInfo.Datasets.Add(datasetID, datasetName);
                    dataPackageInfo.Experiments.Add(datasetID, dataset.Value.Experiment);

                    if (datasetRawFilePaths.TryGetValue(datasetName, out var datasetFilePath))
                    {
                        var datasetFileName = Path.GetFileName(datasetFilePath);
                        mResourceClass.AddResultFileToSkip(datasetFileName);

                        dataPackageInfo.DatasetFiles.Add(datasetID, datasetFileName);

                        dataPackageInfo.DatasetFileTypes.Add(datasetID, dataset.Value.IsDirectoryBased
                            ? DataPackageInfo.DIRECTORY_DATASET
                            : DataPackageInfo.FILE_DATASET);

                        dataPackageInfo.DatasetRawDataTypeNames.Add(datasetID, dataset.Value.RawDataType);
                        dataPackageInfo.DatasetStoragePaths.Add(datasetID, dataset.Value.DatasetDirectoryPath);
                        dataPackageInfo.DatasetTypes.Add(datasetID, dataset.Value.DatasetType);
                    }
                    else if (!skipDatasetsWithExistingMzML)
                    {
                        OnWarningEvent("Dataset file not found by RetrieveDataPackageDatasetFiles for dataset " + datasetName);
                    }

                    dataPackageInfo.DatasetExperimentGroup.Add(datasetID, dataset.Value.DatasetExperimentGroup);
                    dataPackageInfo.DatasetMaxQuantFractionNumber.Add(datasetID, dataset.Value.MaxQuantFractionNumber);
                    dataPackageInfo.DatasetMaxQuantParamGroup.Add(datasetID, dataset.Value.MaxQuantParamGroup);
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in RetrieveDataPackageDatasets", ex);
                dataPackageDatasets = new Dictionary<int, DataPackageDatasetInfo>();
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Retrieves the instrument files for the datasets defined for the data package associated with this aggregation job
        /// </summary>
        /// <param name="retrieveMsXmlFiles">
        /// <para>
        /// Set to true to obtain .mzML (or .mzXML) files for the datasets
        /// </para>
        /// <para>
        /// This method will return false if a .mzML or .mzXML file cannot be found for any of the datasets
        /// </para>
        /// </param>
        /// <param name="dataPackageDatasets">
        /// Output: Dataset info for the datasets associated with this data package; keys are Dataset ID, values are data package info
        /// </param>
        /// <param name="datasetRawFilePaths">
        /// <para>
        /// Output: Keys in this dictionary are dataset name, values are paths to the local file or directory for the dataset
        /// </para>
        /// <para>
        /// If skipDatasetsWithExistingMzML is true, datasets will not be added to datasetRawFilePaths if an existing .mzML file is found
        /// </para>
        /// </param>
        /// <param name="progressPercentAtStart">Percent complete value to use for computing incremental progress</param>
        /// <param name="progressPercentAtFinish">Percent complete value to use for computing incremental progress</param>
        /// <param name="skipDatasetsWithExistingMzML">
        /// When true, for each dataset in the data package, if an existing .mzML file can be found,
        /// do not retrieve the .raw file and do not add to datasetRawFilePaths
        /// </param>
        /// <returns>True if success, false if an error</returns>
        public bool RetrieveDataPackageDatasetFiles(
            bool retrieveMsXmlFiles,
            out Dictionary<int, DataPackageDatasetInfo> dataPackageDatasets,
            out Dictionary<string, string> datasetRawFilePaths,
            float progressPercentAtStart,
            float progressPercentAtFinish,
            bool skipDatasetsWithExistingMzML = false)
        {
            ErrorMessage = string.Empty;

            // SQL Server: Data Source=Gigasax;Initial Catalog=DMS_Pipeline
            // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms
            var brokerDbConnectionString = mResourceClass.MgrParams.GetParam("BrokerConnectionString");

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(brokerDbConnectionString, mResourceClass.MgrParams.ManagerName);

            var dataPackageID = mResourceClass.JobParams.GetJobParameter("DataPackageID", -1);

            var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: mResourceClass.TraceMode);
            RegisterEvents(dbTools);

            var dataPackageFileHandler = new DataPackageFileHandler(dbTools, dataPackageID, mResourceClass);
            RegisterEvents(dataPackageFileHandler);

            var success = dataPackageFileHandler.RetrieveDataPackageDatasetFiles(
                retrieveMsXmlFiles, out dataPackageDatasets, out datasetRawFilePaths,
                progressPercentAtStart, progressPercentAtFinish, skipDatasetsWithExistingMzML);

            if (!success)
            {
                ErrorMessage = dataPackageFileHandler.ErrorMessage;
            }

            return success;
        }

        /// <summary>
        /// Retrieve the dataset file for the current dataset
        /// </summary>
        /// <param name="workingDirectory">Working directory</param>
        /// <param name="dataPackageInfo">Data package info</param>
        /// <param name="retrieveMsXmlFile">
        /// <para>
        /// True if this job's settings file indicates to use .mzML (or .mzXML) files instead of the original instrument file
        /// </para>
        /// <para>
        /// In addition, classes AnalysisResourcesPepProtProphet and AnalysisResourcesMSFragger set this to true, regardless of the settings file
        /// </para>
        /// </param>
        /// <param name="dataPackageDatasets">Output: keys are Dataset ID, values are dataset info</param>
        private CloseOutType RetrieveSingleDataset(
            FileSystemInfo workingDirectory,
            DataPackageInfo dataPackageInfo,
            bool retrieveMsXmlFile,
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
                var datasetType = mResourceClass.JobParams.GetParam("DatasetType");

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
                    RawDataType = rawDataTypeName,
                    DatasetType = datasetType
                };

                dataPackageDatasets.Add(datasetID, dataPackageDatasetInfo);

                if (retrieveMsXmlFile)
                {
                    currentTask = "GetMsXmlFile";

                    var msXMLOutputType = mResourceClass.JobParams.GetJobParameter("MSXMLOutputType", string.Empty);

                    var msXmlType = msXMLOutputType.Equals("mzXML", StringComparison.OrdinalIgnoreCase)
                        ? AnalysisResources.MSXMLOutputTypeConstants.mzXML
                        : AnalysisResources.MSXMLOutputTypeConstants.mzML;

                    var resultCode = msXmlType == AnalysisResources.MSXMLOutputTypeConstants.mzXML
                        ? mResourceClass.GetMzXMLFile()
                        : mResourceClass.GetMzMLFile();

                    if (resultCode != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        return resultCode;
                    }

                    string fileExtension;
                    string msXmlDataType;

                    if (msXmlType == AnalysisResources.MSXMLOutputTypeConstants.mzXML)
                    {
                        fileExtension = AnalysisResources.DOT_MZXML_EXTENSION;
                        msXmlDataType = AnalysisResources.RAW_DATA_TYPE_DOT_MZXML_FILES;
                    }
                    else
                    {
                        fileExtension = AnalysisResources.DOT_MZML_EXTENSION;
                        msXmlDataType = AnalysisResources.RAW_DATA_TYPE_DOT_MZML_FILES;
                    }

                    dataPackageInfo.DatasetFiles.Add(datasetID, mResourceClass.DatasetName + fileExtension);
                    dataPackageInfo.DatasetFileTypes.Add(datasetID, DataPackageInfo.FILE_DATASET);
                    dataPackageInfo.DatasetRawDataTypeNames.Add(datasetID, msXmlDataType);
                    dataPackageInfo.DatasetStoragePaths.Add(datasetID, dataPackageDatasetInfo.DatasetDirectoryPath);
                    dataPackageInfo.DatasetTypes.Add(datasetID, dataPackageDatasetInfo.DatasetType);

                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                currentTask = "Get the primary dataset file";
                var retrievalAttempts = 0;
                var directoryLayoutForMyEMSLFiles = MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories;

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

                            if (!rawDataTypeName.ToLower().Equals(AnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES))
                            {
                                directoryLayoutForMyEMSLFiles = MyEMSLReader.Downloader.DownloadLayout.SingleDataset;
                            }

                            break;

                        case AnalysisResources.RAW_DATA_TYPE_BRUKER_TOF_TDF_FOLDER:
                            // Retrieve the .D directory
                            currentTask = string.Format("Retrieve .D directory; instrument: {0}", instrumentName);
                            var dotDSuccessTDF = mResourceClass.FileSearchTool.RetrieveDotDFolder(false, skipBafAndTdfFiles: false);

                            if (!dotDSuccessTDF)
                                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;

                            // Override the directory layout for MyEMSL files
                            directoryLayoutForMyEMSLFiles = MyEMSLReader.Downloader.DownloadLayout.SingleDataset;
                            break;

                        case AnalysisResources.RAW_DATA_TYPE_DOT_UIMF_FILES:
                            // Check whether the dataset directory has an Agilent .D directory
                            // If it does, and if PreferUIMF is false, retrieve it; otherwise, retrieve the .UIMF file
                            // Instruments IMS08_AgQTOF05 and IMS09_AgQToF06 should have .D directories

                            var isAgilentDotD = DatasetHasAgilentDotD(mResourceClass.DatasetName);
                            var preferUIMF = mResourceClass.JobParams.GetJobParameter("PreferUIMF", false);

                            if (isAgilentDotD && !preferUIMF)
                            {
                                // Retrieve the .D directory
                                currentTask = string.Format("Retrieve .D directory; instrument: {0}", instrumentName);
                                var dotDSuccess = mResourceClass.FileSearchTool.RetrieveDotDFolder(false, skipBafAndTdfFiles: true);

                                if (!dotDSuccess)
                                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;

                                mResourceClass.JobParams.AddAdditionalParameter("DatasetFileRetriever", "ProcessingAgilentDotD", true);

                                // Override the raw data type
                                rawDataType = AnalysisResources.RawDataTypeConstants.AgilentDFolder;
                                rawDataTypeName = AnalysisResources.RAW_DATA_TYPE_DOT_D_FOLDERS;
                                directoryLayoutForMyEMSLFiles = MyEMSLReader.Downloader.DownloadLayout.SingleDataset;
                            }
                            else
                            {
                                // Retrieve the .uimf file for these
                                currentTask = string.Format("Retrieve .UIMF file; instrument: {0}", instrumentName);
                                var uimfResult = GetDatasetFile(rawDataTypeName);

                                if (uimfResult == CloseOutType.CLOSEOUT_FILE_NOT_FOUND)
                                    return uimfResult;

                                mResourceClass.JobParams.AddAdditionalParameter("DatasetFileRetriever", "ProcessingAgilentDotD", false);
                            }

                            break;

                        default:
                            if (string.IsNullOrWhiteSpace(rawDataTypeName))
                            {
                                ErrorMessage = "Job parameter RawDataType is not defined (DatasetFileRetriever.RetrieveSingleDataset)";
                            }
                            else
                            {
                                ErrorMessage = "Dataset type " + rawDataTypeName + " is not supported";

                                OnDebugEvent(
                                    "DatasetFileRetriever.RetrieveSingleDataset: " + ErrorMessage + "; must be " +
                                    AnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES + ", " +
                                    AnalysisResources.RAW_DATA_TYPE_DOT_D_FOLDERS + ", " +
                                    AnalysisResources.RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER + ", " +
                                    AnalysisResources.RAW_DATA_TYPE_BRUKER_FT_FOLDER + ", " +
                                    AnalysisResources.RAW_DATA_TYPE_BRUKER_TOF_TDF_FOLDER + ", or " +
                                    AnalysisResources.RAW_DATA_TYPE_DOT_UIMF_FILES
                                    );
                            }

                            return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }

                    var datasetFileOrDirectoryName = GetDatasetFileOrDirectoryName(rawDataType, out var isDirectory);

                    dataPackageInfo.DatasetFiles.Add(datasetID, datasetFileOrDirectoryName);
                    dataPackageInfo.DatasetFileTypes.Add(datasetID, isDirectory ? "Directory" : DataPackageInfo.FILE_DATASET);
                    dataPackageInfo.DatasetRawDataTypeNames.Add(datasetID, rawDataTypeName);
                    dataPackageInfo.DatasetStoragePaths.Add(datasetID, dataPackageDatasetInfo.DatasetDirectoryPath);
                    dataPackageInfo.DatasetTypes.Add(datasetID, dataPackageDatasetInfo.DatasetType);

                    dataPackageDatasetInfo.IsDirectoryBased = isDirectory;

                    if (mResourceClass.MyEMSLUtils.FilesToDownload.Count == 0)
                    {
                        break;
                    }

                    currentTask = "ProcessMyEMSLDownloadQueue";

                    if (mResourceClass.MyEMSLUtils.ProcessMyEMSLDownloadQueue(workingDirectory.FullName, directoryLayoutForMyEMSLFiles))
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
                OnErrorEvent("Error in RetrieveSingleDataset (CurrentTask = " + currentTask + ")", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }
    }
}
