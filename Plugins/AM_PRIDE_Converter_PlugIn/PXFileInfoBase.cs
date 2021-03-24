using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerPRIDEConverterPlugIn
{
    /// <summary>
    /// ProteomeXchange file info base class
    /// </summary>
    public class PXFileInfoBase
    {
        // Ignore Spelling: ProteomeXchange

        #region "Structures and Enums"

        /// <summary>
        /// ProteomeXChange file type
        /// </summary>
        public enum PXFileTypes
        {
            /// <summary>
            /// Undefined
            /// </summary>
            Undefined = 0,

            /// <summary>
            /// .msgf-pride.xml files
            /// </summary>
            Result = 1,

            /// <summary>
            /// .mzid.gz files from MS-GF+  (listed as "result" files in the .px file)
            /// </summary>
            /// <remarks>Older MS-GF+ jobs have .mzid.zip files</remarks>
            ResultMzId = 2,

            /// <summary>
            /// Instrument data files (typically .raw files)
            /// </summary>
            Raw = 3,

            /// <summary>
            /// Search engine output files, such as Mascot DAT or other output files (from analysis pipelines, such as pep.xml or prot.xml).
            /// </summary>
            Search = 4,

            /// <summary>
            /// _dta.txt or .mgf files
            /// </summary>
            Peak = 5
        }

        #endregion

        #region "Auto-properties"

        /// <summary>
        /// File ID
        /// </summary>
        public int FileID { get; set; }

        /// <summary>
        /// File size (in bytes)
        /// </summary>
        public long Length { get; set; }

        /// <summary>
        /// MD5 hash
        /// </summary>
        public string MD5Hash { get; set; }

        #endregion

        #region "Properties"

        /// <summary>
        /// Filename
        /// </summary>
        public string Filename { get; private set; }

        /// <summary>
        /// Job Info
        /// </summary>
        public DataPackageJobInfo JobInfo { get; private set; }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="dataPkgJob"></param>
        public PXFileInfoBase(string fileName, DataPackageJobInfo dataPkgJob)
        {
            Filename = fileName;
            JobInfo = dataPkgJob;
        }

        /// <summary>
        /// Update Filename, JobInfo, FileID, Length, and MD5 hash
        /// </summary>
        /// <param name="oSource"></param>
        public void Update(PXFileInfoBase oSource)
        {
            Filename = oSource.Filename;
            JobInfo = oSource.JobInfo;
            FileID = oSource.FileID;
            Length = oSource.Length;
            MD5Hash = oSource.MD5Hash;
        }

        /// <summary>
        /// Returns Job number and filename
        /// </summary>
        public override string ToString()
        {
            return "Job " + JobInfo.Job + ": " + Filename;
        }
    }
}
