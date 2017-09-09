using System.Collections.Generic;
using AnalysisManagerBase;

namespace AnalysisManagerPRIDEConverterPlugIn
{
    /// <summary>
    /// ProteomeXchange file info
    /// </summary>
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

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="dataPkgJob"></param>
        public clsPXFileInfo(string fileName, clsDataPackageJobInfo dataPkgJob) : base(fileName, dataPkgJob)
        {
            mFileMappings = new List<int>();
        }

        public void AddFileMapping(int pxFileID)
        /// <summary>
        /// Add a file mapping
        /// </summary>
        /// <param name="parentFileId">Parent FileID</param>
        {
            if (!mFileMappings.Contains(pxFileID))
            {
                mFileMappings.Add(pxFileID);
            }
        }
    }
}
