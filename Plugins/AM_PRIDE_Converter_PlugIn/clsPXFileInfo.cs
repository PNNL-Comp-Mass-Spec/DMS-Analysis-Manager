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

        public List<int> FileMappings => mFileMappings;

        #endregion

        public clsPXFileInfo(string fileName, clsDataPackageJobInfo dataPkgJob) : base(fileName, dataPkgJob)
        {
            mFileMappings = new List<int>();
        }

        public void AddFileMapping(int pxFileID)
        {
            if (!mFileMappings.Contains(pxFileID))
            {
                mFileMappings.Add(pxFileID);
            }
        }
    }
}
