﻿using PHRPReader;

namespace AnalysisManagerBase
{
    /// <summary>
    /// Data package job info
    /// </summary>
    public class clsDataPackageJobInfo
    {
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

        /// <summary>
        /// Analysis tool
        /// </summary>
        public string Tool { get; set; }

        /// <summary>
        /// Number of steps in a split fasta job
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks>0 if not a split fasta job</remarks>
        public int NumberOfClonedSteps { get; set; }

        /// <summary>
        /// Resutl type
        /// </summary>
        public string ResultType { get; set; }

        /// <summary>
        /// PeptideHit result type
        /// </summary>
        public clsPHRPReader.ePeptideHitResultType PeptideHitResultType { get; set; }

        /// <summary>
        /// Settings file name
        /// </summary>
        public string SettingsFileName { get; set; }

        /// <summary>
        /// Parameter file name
        /// </summary>
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

        /// <summary>
        /// Legacy FASTA file name
        /// </summary>
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
        /// The folder (on the storage server) just above the dataset folder
        /// </summary>
        /// <returns></returns>
        public string ServerStoragePath { get; set; }

        /// <summary>
        /// The folder (in the archive) just above the dataset folder
        /// </summary>
        /// <returns></returns>
        public string ArchiveStoragePath { get; set; }

        /// <summary>
        /// Results folder name
        /// </summary>
        public string ResultsFolderName { get; set; }

        /// <summary>
        /// Dataset folder name
        /// </summary>
        public string DatasetFolderName { get; set; }

        /// <summary>
        /// Shared results folder path
        /// </summary>
        public string SharedResultsFolder { get; set; }

        /// <summary>
        /// Instrument data type
        /// </summary>
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
            PeptideHitResultType = clsPHRPReader.ePeptideHitResultType.Unknown;
            SettingsFileName = string.Empty;
            ParameterFileName = string.Empty;
            OrganismDBName = string.Empty;
            LegacyFastaFileName = string.Empty;
            ProteinCollectionList = string.Empty;
            ProteinOptions = string.Empty;
            ServerStoragePath = string.Empty;
            ArchiveStoragePath = string.Empty;
            ResultsFolderName = string.Empty;
            DatasetFolderName = string.Empty;
            SharedResultsFolder = string.Empty;
            RawDataType = string.Empty;
        }

        /// <summary>
        /// Return job number and dataset name
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return Job + ": " + Dataset;
        }
    }
}