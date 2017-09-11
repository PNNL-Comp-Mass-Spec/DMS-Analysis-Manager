using AnalysisManagerBase;

namespace AnalysisManagerPRIDEConverterPlugIn
{
    /// <summary>
    /// ProteomeXchange file info base class
    /// </summary>
    public class clsPXFileInfoBase
    {

        #region "Structures and Enums"

        /// <summary>
        /// ProteomeXhcnage file type
        /// </summary>
        public enum ePXFileType
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
            /// .mzid.gz files from MSGF+  (listed as "result" files in the .px file)
            /// </summary>
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
        public clsDataPackageJobInfo JobInfo { get; private set; }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="dataPkgJob"></param>
        public clsPXFileInfoBase(string fileName, clsDataPackageJobInfo dataPkgJob)
        {
            Filename = fileName;
            JobInfo = dataPkgJob;
        }

        /// <summary>
        /// Update Filename, JobInfo, FileID, Length, and MD5 hash
        /// </summary>
        /// <param name="oSource"></param>
        public void Update(clsPXFileInfoBase oSource)
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
        /// <returns></returns>
        public override string ToString()
        {
            return "Job " + JobInfo.Job + ": " + Filename;
        }
    }
}
