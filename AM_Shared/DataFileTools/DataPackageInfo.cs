using System.Collections.Generic;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using PRISM;

namespace AnalysisManagerBase.DataFileTools
{
    /// <summary>
    /// Data package info
    /// </summary>
    public class DataPackageInfo : EventNotifier
    {
        // Ignore Spelling: Quant, quantitation

        /// <summary>
        /// Value to store in dictionary DatasetFileTypes when the primary instrument data is a single file
        /// </summary>
        public const string FILE_DATASET = "File";

        /// <summary>
        /// Value to store in dictionary DatasetFileTypes when the primary instrument data is a directory of files
        /// </summary>
        public const string DIRECTORY_DATASET = "Directory";

        /// <summary>
        /// Packed parameter DataPackageDatasets
        /// </summary>
        /// <remarks>
        /// Tracks datasets
        /// Keys are dataset name
        /// Values are dataset ID
        /// </remarks>
        private const string JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASETS = "PackedParam_DataPackageDatasets";

        /// <summary>
        /// Packed parameter DataPackageDatasetFiles
        /// </summary>
        /// <remarks>
        /// Tracks dataset file or directory names
        /// Keys are dataset IDs
        /// Values are a file or directory name
        /// </remarks>
        private const string JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_FILES = "PackedParam_DataPackageDatasetFiles";

        /// <summary>
        /// Packed parameter PackedParam_DatasetFileTypes
        /// </summary>
        /// <remarks>
        /// Tracks whether each item in PackedParam_DatasetFilePaths is a File or a Directory
        /// Keys are dataset IDs
        /// Values are the word File or Directory
        /// </remarks>
        private const string JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_FILE_TYPES = "PackedParam_DatasetFileTypes";

        /// <summary>
        /// Packed parameter PackedParam_DatasetRawDataTypes
        /// </summary>
        /// <remarks>
        /// Tracks the raw data type of each dataset
        /// Keys are dataset IDs
        /// Values are raw data type name
        /// </remarks>
        private const string JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_RAW_DATA_TYPES = "PackedParam_DatasetRawDataTypeNames";

        /// <summary>
        /// Packed parameter x
        /// </summary>
        /// <remarks>
        /// Tracks the dataset directory on the storage server for each dataset
        /// Keys are dataset IDs
        /// Values are storage path
        /// </remarks>
        private const string JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_STORAGE_PATHS = "PackedParam_DatasetStoragePaths";

        /// <summary>
        /// Packed parameter PackedParam_DatasetMaxQuantFractionNumbers
        /// </summary>
        /// <remarks>
        /// Tracks the MaxQuant Fraction Numbers for datasets
        /// Keys are dataset IDs
        /// Values are an integer (stored as a string)
        /// </remarks>
        private const string JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_MAX_QUANT_FRACTION_NUMBERS = "PackedParam_DatasetMaxQuantFractionNumbers";

        /// <summary>
        /// Packed parameter PackedParam_DatasetMaxQuantParamGroups
        /// </summary>
        /// <remarks>
        /// Tracks the MaxQuant Parameter Group Index for datasets
        /// Keys are dataset IDs
        /// Values are an integer (stored as a string)
        /// </remarks>
        private const string JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_MAX_QUANT_PARAM_GROUPS = "PackedParam_DatasetMaxQuantParamGroups";

        /// <summary>
        /// Packed parameter PackedParam_DatasetExperimentGroups
        /// </summary>
        /// <remarks>
        /// Tracks the Experiment Group names for datasets (used by MSFragger)
        /// Keys are dataset IDs
        /// Values are group names, or an empty string if no group
        /// </remarks>
        private const string JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_EXPERIMENT_GROUPS = "PackedParam_DatasetExperimentGroups";

        /// <summary>
        /// Packed parameter DataPackageExperiments
        /// </summary>
        /// <remarks>
        /// Tracks experiment names
        /// Keys are dataset IDs
        /// Values are experiment names
        /// </remarks>
        private const string JOB_PARAM_DICTIONARY_DATA_PACKAGE_EXPERIMENTS = "PackedParam_DataPackageExperiments";

        /// <summary>
        /// Data Package ID
        /// </summary>
        public int DataPackageID { get; }

        /// <summary>
        /// List of datasets to process
        /// Keys are dataset IDs
        /// Values are dataset names
        /// </summary>
        public Dictionary<int, string> Datasets { get; }

        /// <summary>
        /// Keys are dataset IDs
        /// Values are experiment names
        /// </summary>
        public Dictionary<int, string> Experiments { get; }

        /// <summary>
        /// Keys are dataset IDs
        /// Values are relative paths to the local file or directory for the dataset
        /// </summary>
        /// <remarks>File or directory name only, not the full path</remarks>
        public Dictionary<int, string> DatasetFiles { get; }

        /// <summary>
        /// Keys are dataset IDs
        /// Values are either File or Directory, indicating the file system type of the dataset file/directory
        /// </summary>
        public Dictionary<int, string> DatasetFileTypes { get; }

        /// <summary>
        /// Keys are dataset IDs
        /// Values are instrument data type name description, e.g. dot_raw_files
        /// </summary>
        public Dictionary<int, string> DatasetRawDataTypeNames { get; }

        /// <summary>
        /// Keys are dataset IDs
        /// Values are paths to the dataset directory on the storage server
        /// </summary>
        public Dictionary<int, string> DatasetStoragePaths { get; }

        /// <summary>
        /// Keys are dataset IDs
        /// Values are the MaxQuant fraction number read from the Package Comment field for the dataset
        /// </summary>
        /// <remarks>
        /// <para>
        /// Value is 0 if the Package Comment field does not have a MaxQuant Fraction defined
        /// </para>
        /// <para>
        /// Fraction numbers are used during Match Between Runs to determine which datasets to examine to find additional PSMs
        /// </para>
        /// </remarks>
        public Dictionary<int, int> DatasetMaxQuantFractionNumber { get; }

        /// <summary>
        /// Keys are dataset IDs
        /// Values are the MaxQuant parameter group index or number read from the Package Comment field for the dataset
        /// </summary>
        /// <remarks>
        /// <para>
        /// Value is 0 if the Package Comment field does not have a MaxQuant Group defined
        /// </para>
        /// <para>
        /// Group Index is defined with "MaxQuant Group 0" or "MaxQuant Group 1" in the Package Comment field
        /// </para>
        /// <para>
        /// Parameter groups are most commonly used to group datasets when using label-free quantitation (LFQ).
        /// Datasets grouped together will be normalized together.
        /// </para>
        /// </remarks>
        public Dictionary<int, int> DatasetMaxQuantParamGroup { get; }

        /// <summary>
        /// Keys are dataset IDs
        /// Values are the MaxQuant or MSFragger Experiment Group from the Package Comment field for the dataset
        /// </summary>
        /// <remarks>
        /// <para>
        /// Value is an empty string if the Package Comment field does not have an Experiment Group defined
        /// </para>
        /// <para>
        /// Experiment group is defined with "FragPipe Group QC_Shew_20_01", "MSFragger Group QC_Shew_20_01", or "MaxQuant Experiment QC_Shew_16_01a" in the Package Comment field
        /// </para>
        /// <para>
        /// Experiment group names can have letters, numbers, or underscores
        /// </para>
        /// </remarks>
        public Dictionary<int, string> DatasetExperimentGroup { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dataPackageID"></param>
        public DataPackageInfo(int dataPackageID)
        {
            DataPackageID = dataPackageID;

            Datasets = new Dictionary<int, string>();
            Experiments = new Dictionary<int, string>();
            DatasetFiles = new Dictionary<int, string>();
            DatasetFileTypes = new Dictionary<int, string>();
            DatasetRawDataTypeNames = new Dictionary<int, string>();
            DatasetStoragePaths = new Dictionary<int, string>();
            DatasetMaxQuantFractionNumber = new Dictionary<int, int>();
            DatasetMaxQuantParamGroup = new Dictionary<int, int>();
            DatasetExperimentGroup = new Dictionary<int, string>();
        }

        /// <summary>
        /// Constructor that loads metadata from packaged job parameter dictionaries
        /// </summary>
        /// <param name="dataPackageID"></param>
        /// <param name="toolRunner"></param>
        /// <param name="warnIfMissingFileInfo">When true, warn if dataset file info is missing</param>
        public DataPackageInfo(int dataPackageID, AnalysisToolRunnerBase toolRunner, bool warnIfMissingFileInfo = true)
        {
            DataPackageID = dataPackageID;

            // Unpack parameters
            Datasets = toolRunner.ExtractPackedJobParameterDictionaryIntegerKey(JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASETS);
            Experiments = toolRunner.ExtractPackedJobParameterDictionaryIntegerKey(JOB_PARAM_DICTIONARY_DATA_PACKAGE_EXPERIMENTS);
            DatasetFiles = toolRunner.ExtractPackedJobParameterDictionaryIntegerKey(JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_FILES);
            DatasetFileTypes = toolRunner.ExtractPackedJobParameterDictionaryIntegerKey(JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_FILE_TYPES);
            DatasetRawDataTypeNames = toolRunner.ExtractPackedJobParameterDictionaryIntegerKey(JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_RAW_DATA_TYPES);
            DatasetStoragePaths = toolRunner.ExtractPackedJobParameterDictionaryIntegerKey(JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_STORAGE_PATHS);
            DatasetMaxQuantFractionNumber = new Dictionary<int, int>();
            DatasetMaxQuantParamGroup = new Dictionary<int, int>();
            DatasetExperimentGroup = new Dictionary<int, string>();

            foreach (var item in toolRunner.ExtractPackedJobParameterDictionaryIntegerKey(JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_MAX_QUANT_FRACTION_NUMBERS))
            {
                DatasetMaxQuantFractionNumber.Add(item.Key, int.TryParse(item.Value, out var fractionNumber) ? fractionNumber : 0);
            }

            foreach (var item in toolRunner.ExtractPackedJobParameterDictionaryIntegerKey(JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_MAX_QUANT_PARAM_GROUPS))
            {
                DatasetMaxQuantParamGroup.Add(item.Key, int.TryParse(item.Value, out var paramGroupIndex) ? paramGroupIndex : 0);
            }

            foreach (var item in toolRunner.ExtractPackedJobParameterDictionaryIntegerKey(JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_EXPERIMENT_GROUPS))
            {
                DatasetExperimentGroup.Add(item.Key, item.Value);
            }

            AddKeysIfMissing(warnIfMissingFileInfo);
        }

        /// <summary>
        /// Constructor that loads metadata from dictionary dataPackageDatasets
        /// </summary>
        /// <param name="dataPackageID"></param>
        /// <param name="dataPackageDatasets"></param>
        /// <param name="warnIfMissingFileInfo">When true, warn if dataset file info is missing</param>
        public DataPackageInfo(int dataPackageID, Dictionary<int, DataPackageDatasetInfo> dataPackageDatasets, bool warnIfMissingFileInfo = true)
        {
            DataPackageID = dataPackageID;

            Datasets = new Dictionary<int, string>();
            Experiments = new Dictionary<int, string>();
            DatasetFiles = new Dictionary<int, string>();
            DatasetFileTypes = new Dictionary<int, string>();
            DatasetRawDataTypeNames = new Dictionary<int, string>();
            DatasetStoragePaths = new Dictionary<int, string>();
            DatasetMaxQuantFractionNumber = new Dictionary<int, int>();
            DatasetMaxQuantParamGroup = new Dictionary<int, int>();
            DatasetExperimentGroup = new Dictionary<int, string>();

            foreach (var item in dataPackageDatasets)
            {
                var datasetId = item.Key;
                var datasetInfo = item.Value;

                Datasets.Add(datasetId, datasetInfo.Dataset);
                Experiments.Add(datasetId, datasetInfo.Experiment);
                DatasetFiles.Add(datasetId, string.Empty);
                DatasetFileTypes.Add(datasetId, datasetInfo.IsDirectoryBased ? DIRECTORY_DATASET : FILE_DATASET);
                DatasetRawDataTypeNames.Add(datasetId, datasetInfo.RawDataType);
                DatasetStoragePaths.Add(datasetId, datasetInfo.DatasetDirectoryPath);
                DatasetMaxQuantFractionNumber.Add(datasetId, datasetInfo.MaxQuantFractionNumber);
                DatasetMaxQuantParamGroup.Add(datasetId, datasetInfo.MaxQuantParamGroup);
                DatasetExperimentGroup.Add(datasetId, datasetInfo.DatasetExperimentGroup);
            }

            AddKeysIfMissing(warnIfMissingFileInfo);
        }

        private void AddKeyIfMissing(string dictionaryName, IDictionary<int, int> targetDictionary, int datasetId, bool warnIfMissing = true)
        {
            if (targetDictionary.ContainsKey(datasetId))
                return;

            if (warnIfMissing)
            {
                OnWarningEvent("For data package {0}, dictionary {1} is missing DatasetID {2}", DataPackageID, dictionaryName, datasetId);
            }

            targetDictionary.Add(datasetId, 0);
        }

        private void AddKeyIfMissing(string dictionaryName, IDictionary<int, string> targetDictionary, int datasetId, bool warnIfMissing = true)
        {
            if (targetDictionary.ContainsKey(datasetId))
                return;

            if (warnIfMissing)
            {
                OnWarningEvent("For data package {0}, dictionary {1} is missing DatasetID {2}", DataPackageID, dictionaryName, datasetId);
            }

            targetDictionary.Add(datasetId, string.Empty);
        }

        private void AddKeysIfMissing(bool warnIfMissingFileInfo)
        {
            var hasDataPackage = DataPackageID > 0;

            // Assure that the dictionaries contain all of the dataset IDs in the Datasets dictionary
            foreach (var datasetId in Datasets.Keys)
            {
                AddKeyIfMissing("Experiments", Experiments, datasetId);
                AddKeyIfMissing("DatasetFiles", DatasetFiles, datasetId, warnIfMissingFileInfo);
                AddKeyIfMissing("DatasetFileTypes", DatasetFileTypes, datasetId, warnIfMissingFileInfo);
                AddKeyIfMissing("DatasetRawDataTypeNames", DatasetRawDataTypeNames, datasetId, warnIfMissingFileInfo);
                AddKeyIfMissing("DatasetStoragePaths", DatasetStoragePaths, datasetId, warnIfMissingFileInfo);
                AddKeyIfMissing("DatasetMaxQuantFractionNumber", DatasetMaxQuantFractionNumber, datasetId, hasDataPackage);
                AddKeyIfMissing("DatasetMaxQuantParamGroup", DatasetMaxQuantParamGroup, datasetId, hasDataPackage);
                AddKeyIfMissing("DatasetExperimentGroup", DatasetExperimentGroup, datasetId, hasDataPackage);
            }
        }

        /// <summary>
        /// Clear cached data
        /// </summary>
        public void Clear()
        {
            Datasets.Clear();
            Experiments.Clear();
            DatasetFiles.Clear();
            DatasetFileTypes.Clear();
            DatasetRawDataTypeNames.Clear();
            DatasetStoragePaths.Clear();
            DatasetMaxQuantFractionNumber.Clear();
            DatasetMaxQuantParamGroup.Clear();
            DatasetExperimentGroup.Clear();
        }

        /// <summary>
        /// Obtain a dictionary with information about this data package's datasets
        /// </summary>
        /// <remarks>
        /// Will include dataset name, ID, experiment dataset directory path, MaxQuant parameter group ID,
        /// and dataset experiment group (from the data package comment field for each dataset)
        /// </remarks>
        /// <returns>Dictionary where keys are Dataset IDs and values are dataset info</returns>
        public Dictionary<int, DataPackageDatasetInfo> GetDataPackageDatasets()
        {
            var dataPackageDatasets = new Dictionary<int, DataPackageDatasetInfo>();

            foreach (var dataset in Datasets)
            {
                var datasetId = dataset.Key;

                var datasetInfo = new DataPackageDatasetInfo(dataset.Value, datasetId)
                {
                    DatasetDirectoryPath = DatasetStoragePaths[datasetId],
                    DatasetExperimentGroup = DatasetExperimentGroup[datasetId],
                    Experiment = Experiments[datasetId],
                    IsDirectoryBased = DatasetFileTypes[datasetId].Equals("Directory"),
                    MaxQuantFractionNumber = DatasetMaxQuantFractionNumber[datasetId],
                    MaxQuantParamGroup = DatasetMaxQuantParamGroup[datasetId],
                    RawDataType = DatasetRawDataTypeNames[datasetId]
                };

                // Skip: datasetInfo.DatasetFileName = DatasetFiles[datasetId];

                dataPackageDatasets.Add(datasetId, datasetInfo);
            }

            return dataPackageDatasets;
        }

        /// <summary>
        /// Store data package info a packed job parameters
        /// </summary>
        /// <param name="analysisResources"></param>
        public void StorePackedDictionaries(AnalysisResources analysisResources)
        {
            analysisResources.StorePackedJobParameterDictionary(Datasets, JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASETS);
            analysisResources.StorePackedJobParameterDictionary(Experiments, JOB_PARAM_DICTIONARY_DATA_PACKAGE_EXPERIMENTS);
            analysisResources.StorePackedJobParameterDictionary(DatasetFiles, JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_FILES);
            analysisResources.StorePackedJobParameterDictionary(DatasetFileTypes, JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_FILE_TYPES);
            analysisResources.StorePackedJobParameterDictionary(DatasetRawDataTypeNames, JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_RAW_DATA_TYPES);
            analysisResources.StorePackedJobParameterDictionary(DatasetStoragePaths, JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_STORAGE_PATHS);
            analysisResources.StorePackedJobParameterDictionary(DatasetMaxQuantFractionNumber, JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_MAX_QUANT_FRACTION_NUMBERS);
            analysisResources.StorePackedJobParameterDictionary(DatasetMaxQuantParamGroup, JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_MAX_QUANT_PARAM_GROUPS);
            analysisResources.StorePackedJobParameterDictionary(DatasetExperimentGroup, JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_EXPERIMENT_GROUPS);
        }
    }
}
