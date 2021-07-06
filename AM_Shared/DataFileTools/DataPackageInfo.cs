using System.Collections.Generic;
using AnalysisManagerBase.AnalysisTool;

namespace AnalysisManagerBase.DataFileTools
{
    /// <summary>
    /// Data package info
    /// </summary>
    public class DataPackageInfo
    {
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
        /// Packed parameter PackedParam_DatasetMaxQuantParamGroups
        /// </summary>
        /// <remarks>
        /// Tracks the MaxQuant Parameter Group Index for datasets
        /// Keys are dataset IDs
        /// Values are an integer (stored as a string)
        /// </remarks>
        private const string JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_MAX_QUANT_PARAM_GROUP = "PackedParam_DatasetMaxQuantParamGroups";

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
        /// Values are paths to the local file or directory for the dataset
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
        /// Values are the MaxQuant parameter group index or number read from the Package Comment field for the dataset
        /// </summary>
        /// <remarks>Will be 0 if the Package Comment field does not have a MaxQuant Group defined</remarks>
        public Dictionary<int, int> DatasetMaxQuantParamGroup { get; }

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
            DatasetMaxQuantParamGroup = new Dictionary<int, int>();
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dataPackageID"></param>
        /// <param name="toolRunner"></param>
        public DataPackageInfo(int dataPackageID, AnalysisToolRunnerBase toolRunner)
        {
            DataPackageID = dataPackageID;

            // Unpack parameters
            Datasets = toolRunner.ExtractPackedJobParameterDictionaryIntegerKey(JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASETS);
            Experiments = toolRunner.ExtractPackedJobParameterDictionaryIntegerKey(JOB_PARAM_DICTIONARY_DATA_PACKAGE_EXPERIMENTS);
            DatasetFiles = toolRunner.ExtractPackedJobParameterDictionaryIntegerKey(JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_FILES);
            DatasetFileTypes = toolRunner.ExtractPackedJobParameterDictionaryIntegerKey(JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_FILE_TYPES);
            DatasetMaxQuantParamGroup = new Dictionary<int, int>();

            foreach (var item in toolRunner.ExtractPackedJobParameterDictionaryIntegerKey(JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_MAX_QUANT_PARAM_GROUP))
            {
                DatasetMaxQuantParamGroup.Add(item.Key, int.TryParse(item.Value, out var paramGroupIndex) ? paramGroupIndex : 0);
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
            DatasetMaxQuantParamGroup.Clear();
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
            analysisResources.StorePackedJobParameterDictionary(DatasetMaxQuantParamGroup, JOB_PARAM_DICTIONARY_DATA_PACKAGE_DATASET_MAX_QUANT_PARAM_GROUP);
        }
    }
}
