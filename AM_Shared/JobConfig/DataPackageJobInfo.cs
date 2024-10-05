using System.Collections.Generic;
using PHRPReader;

namespace AnalysisManagerBase.JobConfig
{
    /// <summary>
    /// Data package job info
    /// </summary>
    public class DataPackageJobInfo
    {
        // Ignore Spelling: FASTA, na

        /// <summary>
        /// Analysis job
        /// </summary>
        public int Job { get; }

        /// <summary>
        /// Dataset name
        /// </summary>
        public string Dataset { get; }

        /// <summary>
        /// Dataset ID
        /// </summary>
        public int DatasetID { get; set; }

        /// <summary>
        /// Instrument name
        /// </summary>
        public string Instrument { get; set; }

        /// <summary>
        /// Instrument group
        /// </summary>
        public string InstrumentGroup { get; set; }

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
        /// NEWT ID for Experiment_Organism; see https://dms2.pnl.gov/ontology/report/NEWT/
        /// </summary>
        public int Experiment_NEWT_ID { get; set; }

        /// <summary>
        /// NEWT Name for Experiment_Organism; see https://dms2.pnl.gov/ontology/report/NEWT/
        /// </summary>
        public string Experiment_NEWT_Name { get; set; }

        /// <summary>
        /// Analysis tool
        /// </summary>
        public string Tool { get; set; }

        /// <summary>
        /// Number of steps in a split FASTA job
        /// </summary>
        /// <remarks>0 if not a split FASTA job</remarks>
        public int NumberOfClonedSteps { get; set; }

        /// <summary>
        /// Result type
        /// </summary>
        public string ResultType { get; set; }

        /// <summary>
        /// PeptideHit result type
        /// </summary>
        public PeptideHitResultTypes PeptideHitResultType { get; set; }

        /// <summary>
        /// Settings file name
        /// </summary>
        public string SettingsFileName { get; set; }

        /// <summary>
        /// Parameter file name
        /// </summary>
        public string ParameterFileName { get; set; }

        /// <summary>
        /// Generated FASTA File Name
        /// </summary>
        public string GeneratedFASTAFileName { get; set; }

        /// <summary>
        /// Legacy FASTA file name
        /// </summary>
        /// <remarks>
        /// Is initially an empty string, but is typically "na" when using a protein collection list
        /// </remarks>
        public string LegacyFastaFileName { get; set; }

        /// <summary>
        /// Protein collection list
        /// </summary>
        public string ProteinCollectionList { get; set; }

        /// <summary>
        /// Protein collection options
        /// </summary>
        public string ProteinOptions { get; set; }

        /// <summary>
        /// The directory (on the storage server) just above the dataset directory
        /// </summary>
        /// <remarks>
        /// Example value: \\proto-8\QEHFX03\2021_1\
        /// </remarks>
        public string ServerStoragePath { get; set; }

        /// <summary>
        /// The directory (in the archive) just above the dataset directory
        /// </summary>
        public string ArchiveStoragePath { get; set; }

        /// <summary>
        /// Results directory name
        /// </summary>
        public string ResultsFolderName { get; set; }

        /// <summary>
        /// Dataset directory name
        /// </summary>
        public string DatasetFolderName { get; set; }

        /// <summary>
        /// Shared results directory paths
        /// </summary>
        public List<string> SharedResultsFolders { get; set; }

        /// <summary>
        /// Instrument data type
        /// </summary>
        public string RawDataType { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public DataPackageJobInfo(int job, string datasetName)
        {
            Job = job;
            Dataset = datasetName;

            DatasetID = 0;
            Instrument = string.Empty;
            InstrumentGroup = string.Empty;
            Experiment = string.Empty;
            Experiment_Reason = string.Empty;
            Experiment_Comment = string.Empty;
            Experiment_Organism = string.Empty;
            Experiment_NEWT_ID = 0;
            Experiment_NEWT_Name = string.Empty;
            Tool = string.Empty;
            NumberOfClonedSteps = 0;
            ResultType = string.Empty;
            PeptideHitResultType = PeptideHitResultTypes.Unknown;
            SettingsFileName = string.Empty;
            ParameterFileName = string.Empty;
            GeneratedFASTAFileName = string.Empty;
            LegacyFastaFileName = string.Empty;
            ProteinCollectionList = string.Empty;
            ProteinOptions = string.Empty;
            ServerStoragePath = string.Empty;
            ArchiveStoragePath = string.Empty;
            ResultsFolderName = string.Empty;
            DatasetFolderName = string.Empty;
            SharedResultsFolders = new List<string>();
            RawDataType = string.Empty;
        }

        /// <summary>
        /// Show the job number and dataset name
        /// </summary>
        public override string ToString()
        {
            return Job + ": " + Dataset;
        }
    }
}