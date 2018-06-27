
namespace AnalysisManagerBase
{
    /// <summary>
    /// Data package dataset info
    /// </summary>
    public class clsDataPackageDatasetInfo
    {
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
        /// NEWT ID for Experiment_Organism; see http://dms2.pnl.gov/ontology/report/NEWT/
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public int Experiment_NEWT_ID { get; set; }

        /// <summary>
        /// NEWT Name for Experiment_Organism; see https://dms2.pnl.gov/ontology/report/NEWT/
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public string Experiment_NEWT_Name { get; set; }

        /// <summary>
        /// Dataset folder path
        /// </summary>
        public string ServerStoragePath { get; set; }

        /// <summary>
        /// Archive folder path
        /// </summary>
        public string ArchiveStoragePath { get; set; }

        /// <summary>
        /// Instrument data type
        /// </summary>
        public string RawDataType { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks></remarks>
        public clsDataPackageDatasetInfo(string datasetName, int datasetId)
        {
            Dataset = datasetName;
            DatasetID = datasetId;

            Instrument = string.Empty;
            InstrumentGroup = string.Empty;
            Experiment = string.Empty;
            Experiment_Reason = string.Empty;
            Experiment_Comment = string.Empty;
            Experiment_Organism = string.Empty;
            Experiment_NEWT_ID = 0;
            Experiment_NEWT_Name = string.Empty;
            ServerStoragePath = string.Empty;
            ArchiveStoragePath = string.Empty;
            RawDataType = string.Empty;

        }

    }
}