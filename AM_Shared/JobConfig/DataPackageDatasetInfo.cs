
namespace AnalysisManagerBase.JobConfig
{
    /// <summary>
    /// Data package dataset info
    /// </summary>
    public class DataPackageDatasetInfo
    {
        // Ignore Spelling: Maxq, Quant, quantitation

        /// <summary>
        /// Dataset name
        /// </summary>
        public string Dataset { get; }

        /// <summary>
        /// Dataset ID
        /// </summary>
        public int DatasetID { get; }

        /// <summary>
        /// Dataset type, e.g. HMS-HCD-MSn, HMS-HCD-HMSn, DIA-HMS-HCD-HMSn, HMS-HCD-CID-MSn
        /// </summary>
        public string DatasetType { get; set; }

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
        /// Dataset experiment group name; empty string if not defined
        /// </summary>
        /// <remarks>
        /// <para>
        /// For MaxQuant, overrides the experiment name for the given dataset (in the MaxQuant parameter file)
        /// </para>
        /// <para>
        /// For MSFragger (and FragPipe), used to roll up quantitation info amongst datasets in the same group
        /// </para>
        /// <para>
        /// aka ExperimentGroup or ExperimentGroupName
        /// </para>
        /// <para>
        /// Example usage: https://dms2.pnl.gov/data_package_dataset/report/3871/-/-/-/-/-
        /// </para>
        /// </remarks>
        /// <example>
        /// The following are example dataset comments that include the experiment group name (CohortA, 1, 10, or 5):
        ///   MSFragger Group CohortA
        ///   MSFragger Group 1
        ///   MSFragger Experiment CohortA
        ///   MSFrag Group CohortA
        ///   MSFrag Group 10
        ///   FragPipe Group CohortA
        ///   FragPipe Group 5
        /// We also match MaxQuant prefixes, in case the same data package is used for both MSFragger and MaxQuant:
        ///   MaxQuant Group CohortA
        ///   MaxQuant Group 5
        ///   MaxQuant Experiment CohortA
        ///   Maxq Group: CohortA
        ///   Maxq Group: 5
        ///   MQ Group CohortA
        ///   MQ Group 5
        /// </example>
        public string DatasetExperimentGroup { get; set; }

        /// <summary>
        /// MaxQuant fraction number (or 0 if undefined)
        /// </summary>
        /// <remarks>
        /// <para>
        /// Fraction numbers are used during Match Between Runs to determine which datasets to examine to find additional PSMs
        /// </para>
        /// <para>
        /// Matching logic:
        ///   Fraction 1 will be matched with all fractions 1 and 2
        ///   Fraction 2 will be matched against all fractions 1, 2, and 3
        ///   Fraction 3 will be matched against all fractions 2, 3, and 4
        /// </para>
        /// <para>
        /// If only the fractions of one sample are to be matched against each other,
        /// but not to the fractions of another sample, introduce gaps between the different groups, e.g.:
        ///   - Use fractions  1,  2,  3, etc. for the first sample group
        ///   - Use fractions 11, 12, 13, etc. for the second sample group
        ///   - Use fractions 21, 22, 23, etc. for the third sample group
        /// </para>
        /// </remarks>
        /// <example>
        /// The following are example dataset comments that include the fraction number (1, 3, or 10):
        ///   MaxQuant Fraction 1
        ///   Maxq Fraction: 3
        ///   MQ Fraction 10
        /// </example>
        public int MaxQuantFractionNumber { get; set; }

        /// <summary>
        /// MaxQuant parameter group index or number (or 0 if undefined)
        /// </summary>
        /// <remarks>
        /// <para>
        /// Parameter groups are most commonly used to group datasets when using label-free quantitation (LFQ).
        /// Datasets grouped together will be normalized together.
        /// </para>
        /// <para>
        /// Example usage: https://dms2.pnl.gov/data_package_dataset/report/3887/-/-/-/-/-
        /// </para>
        /// </remarks>
        /// <example>
        /// The following are example dataset comments that include the parameter group ID (1, 3, or 10):
        ///   MaxQuant Group 1
        ///   Maxq Group: 3
        ///   MQ Group 10
        /// </example>
        public int MaxQuantParamGroup { get; set; }

        /// <summary>
        /// Archive directory path
        /// </summary>
        /// <remarks>This path is not applicable for datasets in MyEMSL</remarks>
        public string DatasetArchivePath { get; set; }

        /// <summary>
        /// Instrument data type name description, e.g. dot_raw_files
        /// </summary>
        public string RawDataType { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public DataPackageDatasetInfo(string datasetName, int datasetId)
        {
            Dataset = datasetName;
            DatasetID = datasetId;
            DatasetType = string.Empty;

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
            MaxQuantFractionNumber = 0;

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