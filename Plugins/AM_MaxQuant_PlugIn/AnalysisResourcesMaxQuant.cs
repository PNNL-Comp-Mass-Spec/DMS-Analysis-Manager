using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase;

namespace AnalysisManagerMaxQuantPlugIn
{
    /// <summary>
    /// Retrieve resources for the MaxQuant plugin
    /// </summary>
    public class AnalysisResourcesMaxQuant : AnalysisResources
    {
        // Ignore Spelling: MaxQuant, Parm

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

                var paramFileName = mJobParams.GetParam(JOB_PARAM_PARAMETER_FILE);
                currentTask = "RetrieveParamFile " + paramFileName;

                // Retrieve param file
                if (!FileSearch.RetrieveFile(paramFileName, mJobParams.GetParam("ParmFileStoragePath")))
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;

                // Retrieve Fasta file
                var orgDbDirectoryPath = mMgrParams.GetParam(MGR_PARAM_ORG_DB_DIR);

                currentTask = "RetrieveOrgDB to " + orgDbDirectoryPath;
                if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                    return resultCode;

                var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);

                var dataPackageInfo = new DataPackageInfo(dataPackageID);

                CloseOutType datasetSuccess;

                if (dataPackageID > 0)
                {
                    datasetSuccess = RetrieveDataPackageDatasets(dataPackageInfo);
                }
                else
                {
                    datasetSuccess = RetrieveSingleDataset(dataPackageInfo);
                }

                if (datasetSuccess != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return datasetSuccess;
                }

                dataPackageInfo.StorePackedDictionaries(this);

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Exception in GetResources (CurrentTask = " + currentTask + ")", ex);
                return CloseOutType.CLOSEOUT_FAILED;
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

        private CloseOutType RetrieveSingleDataset(DataPackageInfo dataPackageInfo)
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
                        if (mMyEMSLUtilities.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
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
