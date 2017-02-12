using System.Collections.Generic;
using AnalysisManagerBase;

namespace AnalysisManagerPRIDEConverterPlugIn
{
    public class clsPXFileInfo : clsPXFileInfoBase
    {
        #region "Module Variables"

        #endregion

        protected readonly List<int> mFileMappings;

        #region "Auto-properties"

        public ePXFileType PXFileType { get; set; }

        #endregion

        #region "Properties"

        public List<int> FileMappings
        {
            get { return mFileMappings; }
        }

        #endregion

        public clsPXFileInfo(string fileName, clsDataPackageJobInfo dataPkgJob) : base(fileName, dataPkgJob)
        {
            mFileMappings = new List<int>();
        }

        public void AddFileMapping(int intPXFileID)
        {
            if (!mFileMappings.Contains(intPXFileID))
            {
                mFileMappings.Add(intPXFileID);
            }
        }
    }
}
