using AnalysisManagerBase;
using System.Collections.Generic;

namespace AnalysisManagerPRIDEConverterPlugIn
{
    /// <summary>
    /// ProteomeXchange file info
    /// </summary>
    public class clsPXFileInfo : clsPXFileInfoBase
    {
        // Ignore Spelling: ProteomeXchange

        #region "Properties"

        /// <summary>
        /// ProteomeXchange file type
        /// </summary>
        public ePXFileType PXFileType { get; set; }

        /// <summary>
        /// Mapping from this file to parent files
        /// </summary>
        public List<int> FileMappings { get; }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="dataPkgJob"></param>
        public clsPXFileInfo(string fileName, clsDataPackageJobInfo dataPkgJob) : base(fileName, dataPkgJob)
        {
            FileMappings = new List<int>();
        }

        /// <summary>
        /// Add a file mapping
        /// </summary>
        /// <param name="parentFileId">Parent FileID</param>
        public void AddFileMapping(int parentFileId)
        {
            if (!FileMappings.Contains(parentFileId))
            {
                FileMappings.Add(parentFileId);
            }
        }
    }
}
