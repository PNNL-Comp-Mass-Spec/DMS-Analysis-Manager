
namespace AnalysisManagerBase
{
    public class clsDataPackageDatasetInfo
    {
        public string Dataset { get; }

        public int DatasetID { get; }

        public string Instrument { get; set; }

        public string InstrumentGroup { get; set; }

        public string Experiment { get; set; }

        public string Experiment_Reason { get; set; }

        public string Experiment_Comment { get; set; }

        public string Experiment_Organism { get; set; }

        /// <summary>
        /// NEWT ID for Experiment_Organism; see http://dms2.pnl.gov/ontology/report/NEWT/
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public int Experiment_NEWT_ID { get; set; }

        /// <summary>
        /// NEWT Name for Experiment_Organism; see http://dms2.pnl.gov/ontology/report/NEWT/
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public string Experiment_NEWT_Name { get; set; }

        public string ServerStoragePath { get; set; }

        public string ArchiveStoragePath { get; set; }

        public string RawDataType { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks></remarks>
        public clsDataPackageDatasetInfo(string datasetName, int datasetId)
        {
            Dataset = datasetName;
            DatasetID = datasetId;

            Instrument = "";
            InstrumentGroup = "";
            Experiment = "";
            Experiment_Reason = "";
            Experiment_Comment = "";
            Experiment_Organism = "";
            Experiment_NEWT_ID = 0;
            Experiment_NEWT_Name = "";
            ServerStoragePath = "";
            ArchiveStoragePath = "";
            RawDataType = "";

        }

    }
}