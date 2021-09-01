
namespace AnalysisManagerBase.JobConfig
{
    /// <summary>
    /// Data package dataset info
    /// </summary>
    public class DataPackageDatasetInfo
    {
        // Ignore Spelling: Maxq

        /// <summary>
        /// Dataset name
        /// </summary>
        public string Dataset { get; }

        /// <summary>
        /// Dataset ID
        /// </summary>
        public int DatasetID { get; }

        /// <summary>
        /// Instrument
        /// </summary>
        public string Instrument { get; set; }

        /// <summary>
        /// Instrument group
        /// </summary>
        public string InstrumentGroup { get; set; }

        /// <summary>
        /// True if the instrument data is stored in a directory (e.g. Agilent .d directories)
        /// False if the instrument data is stored in a single file (e.g. Thermo .raw files)
        /// </summary>
        /// <remarks>
        /// If the instrument data for this aggregation job is stored as a .mzML file,
        /// this property will be false, even for instruments that use Agilent .d directories
        /// </remarks>
        public bool IsDirectoryBased { get; set; }

        /// <summary>
        /// Experiment name
        /// </summary>
        public string Experiment { get; set; }

        /// <summary>
        /// Experiment reason
        /// </summary>
        public string Experiment_Reason { get; set; }

        /// <summary>
        /// Experiment comment
        /// </summary>
        public string Experiment_Comment { get; set; }

        /// <summary>
        /// Experiment organism
        /// </summary>
        public string Experiment_Organism { get; set; }

        /// <summary>
        /// BTO ID for Experiment_Tissue; see https://dms2.pnl.gov/tissue/report
        /// For example, BTO:0000131
        /// </summary>
        public string Experiment_Tissue_ID { get; set; }

        /// <summary>
        /// Experiment_Tissue name; see https://dms2.pnl.gov/tissue/report
        /// For example, blood plasma
        /// </summary>
        public string Experiment_Tissue_Name { get; set; }

        /// <summary>
        /// NEWT ID for Experiment_Organism; see https://dms2.pnl.gov/ontology/report/NEWT/
        /// </summary>
        public int Experiment_NEWT_ID { get; set; }

        /// <summary>
        /// NEWT Name for Experiment_Organism; see https://dms2.pnl.gov/ontology/report/NEWT/
        /// </summary>
        public string Experiment_NEWT_Name { get; set; }

        /// <summary>
        /// Dataset directory path
        /// </summary>
        /// <remarks>
        /// Example value: \\proto-8\QEHFX03\2021_1\QC_Dataset_15March21
        /// </remarks>
        public string DatasetDirectoryPath { get; set; }

        /// <summary>
        /// Data package comment
        /// </summary>
        public string DataPackageComment { get; set; }

        /// <summary>
        /// Dataset experiment group name (used by MSFragger); empty string if not defined
        /// </summary>
        /// <remarks>
        /// Example usage: https://dms2.pnl.gov/data_package_dataset/report/3871/-/-/-/-/-
        /// </remarks>
        /// <example>
        /// The following are example dataset comments that include the experiment group name (CohortA, 1, 10, or 5):
        /// MSFragger Group CohortA
        /// MSFragger Group 1
        /// MSFrag Group CohortA
        /// MSFrag Group 10
        /// FragPipe Group CohortA
        /// FragPipe Group 5
        /// MaxQuant Group CohortA
        /// MaxQuant Group 5
        /// Maxq Group: CohortA
        /// Maxq Group: 5
        /// MQ Group CohortA
        /// MQ Group 5
        /// </example>
        public string DatasetExperimentGroup { get; set; }

        /// <summary>
        /// MaxQuant parameter group index or number (or 0 if undefined)
        /// </summary>
        /// <remarks>
        /// Example usage: https://dms2.pnl.gov/data_package_dataset/report/3833/-/-/-/-/-
        /// </remarks>
        /// <example>
        /// The following are example dataset comments that include the experiment group name (1, 3, or 10):
        /// MaxQuant Group 1
        /// Maxq Group: 3
        /// MQ Group 10
        /// </example>
        public int MaxQuantParamGroup { get; set; }

        /// <summary>
        /// Archive directory path
        /// </summary>
        /// <remarks>This path is not applicable for datasets in MyEMSL</remarks>
        public string DatasetArchivePath { get; set; }

        /// <summary>
        /// Instrument data type name
        /// </summary>
        public string RawDataType { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public DataPackageDatasetInfo(string datasetName, int datasetId)
        {
            Dataset = datasetName;
            DatasetID = datasetId;

            Instrument = string.Empty;
            InstrumentGroup = string.Empty;
            IsDirectoryBased = false;

            Experiment = string.Empty;
            Experiment_Reason = string.Empty;
            Experiment_Comment = string.Empty;
            Experiment_Organism = string.Empty;
            Experiment_Tissue_ID = string.Empty;
            Experiment_Tissue_Name = string.Empty;
            Experiment_NEWT_ID = 0;
            Experiment_NEWT_Name = string.Empty;

            DatasetDirectoryPath = string.Empty;
            DatasetArchivePath = string.Empty;

            DataPackageComment = string.Empty;
            DatasetExperimentGroup = string.Empty;
            MaxQuantParamGroup = 0;

            RawDataType = string.Empty;
        }

        /// <summary>
        /// Show dataset name or ID
        /// </summary>
        public override string ToString()
        {
            return string.IsNullOrWhiteSpace(Dataset)
                ? "Dataset ID " + DatasetID
                : Dataset;
        }
    }
}