using PHRPReader;


namespace AnalysisManagerBase
{
    public class clsDataPackageJobInfo
    {
        public int Job { get; }

        public string Dataset { get; }

        public int DatasetID { get; set; }

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

        public string Tool { get; set; }

        /// <summary>
        /// Number of steps in a split fasta job
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks>0 if not a split fasta job</remarks>
        public int NumberOfClonedSteps { get; set; }

        public string ResultType { get; set; }

        public clsPHRPReader.ePeptideHitResultType PeptideHitResultType { get; set; }

        public string SettingsFileName { get; set; }

        public string ParameterFileName { get; set; }

        /// <summary>
        /// Generated Fasta File Name or legacy fasta file name
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks>
        /// For jobs where ProteinCollectionList = 'na', this is the legacy fasta file name
        /// Otherwise, this is the generated fasta file name (or "na")
        /// </remarks>
        public string OrganismDBName { get; set; }

        public string LegacyFastaFileName { get; set; }

        public string ProteinCollectionList { get; set; }

        public string ProteinOptions { get; set; }

        /// <summary>
        /// The folder (on the storage server) just above the dataset folder
        /// </summary>
        /// <returns></returns>
        public string ServerStoragePath { get; set; }

        /// <summary>
        /// The folder (in the archive) just above the dataset folder
        /// </summary>
        /// <returns></returns>
        public string ArchiveStoragePath { get; set; }

        public string ResultsFolderName { get; set; }

        public string DatasetFolderName { get; set; }

        public string SharedResultsFolder { get; set; }

        public string RawDataType { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks></remarks>
        public clsDataPackageJobInfo(int job, string datasetName)
        {
            Job = job;
            Dataset = datasetName;

            DatasetID = 0;
            Instrument = "";
            InstrumentGroup = "";
            Experiment = "";
            Experiment_Reason = "";
            Experiment_Comment = "";
            Experiment_Organism = "";
            Experiment_NEWT_ID = 0;
            Experiment_NEWT_Name = "";
            Tool = "";
            NumberOfClonedSteps = 0;
            ResultType = "";
            PeptideHitResultType = clsPHRPReader.ePeptideHitResultType.Unknown;
            SettingsFileName = "";
            ParameterFileName = "";
            OrganismDBName = "";
            LegacyFastaFileName = "";
            ProteinCollectionList = "";
            ProteinOptions = "";
            ServerStoragePath = "";
            ArchiveStoragePath = "";
            ResultsFolderName = "";
            DatasetFolderName = "";
            SharedResultsFolder = "";
            RawDataType = "";
        }

    }
}