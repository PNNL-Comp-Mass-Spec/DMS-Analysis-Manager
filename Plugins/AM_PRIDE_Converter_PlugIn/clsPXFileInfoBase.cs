using AnalysisManagerBase;

namespace AnalysisManagerPRIDEConverterPlugIn
{
    public class clsPXFileInfoBase
    {
        #region "Module Variables"

        protected string mFileName;
        protected clsDataPackageJobInfo mJobInfo;

        #endregion

        #region "Structures and Enums"

        public enum ePXFileType
        {
            Undefined = 0,
            Result = 1,              // .msgf-pride.xml files
            ResultMzId = 2,          // .mzid.gz files from MSGF+  (listed as "result" files in the .px file)
            Raw = 3,                 // Instrument data files (typically .raw files)
            Search = 4,              // Search engine output files, such as Mascot DAT or other output files (from analysis pipelines, such as pep.xml or prot.xml).
            Peak = 5                 // _dta.txt or .mgf files
        }

        #endregion

        #region "Auto-properties"

        public int FileID { get; set; }
        public long Length { get; set; }
        public string MD5Hash { get; set; }

        #endregion

        #region "Properties"

        public string Filename
        {
            get { return mFileName; }
        }

        public clsDataPackageJobInfo JobInfo
        {
            get { return mJobInfo; }
        }

        #endregion

        public clsPXFileInfoBase(string fileName, clsDataPackageJobInfo dataPkgJob)
        {
            mFileName = fileName;
            mJobInfo = dataPkgJob;
        }

        public void Update(clsPXFileInfoBase oSource)
        {
            this.mFileName = oSource.mFileName;
            this.mJobInfo = oSource.mJobInfo;
            this.FileID = oSource.FileID;
            this.Length = oSource.Length;
            this.MD5Hash = oSource.MD5Hash;
        }

        public override string ToString()
        {
            return "Job " + mJobInfo.Job + ": " + mFileName;
        }
    }
}
