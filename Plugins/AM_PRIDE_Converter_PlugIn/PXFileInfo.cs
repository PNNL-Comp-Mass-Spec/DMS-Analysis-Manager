using System.Collections.Generic;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerPRIDEConverterPlugIn
{
    /// <summary>
    /// ProteomeXchange file info
    /// </summary>
    public class PXFileInfo : PXFileInfoBase
    {
        // Ignore Spelling: ProteomeXchange

        /// <summary>
        /// ProteomeXchange file type
        /// </summary>
        public PXFileTypes PXFileType { get; set; }

        /// <summary>
        /// Mapping from this file to parent files
        /// </summary>
        public List<int> FileMappings { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="dataPkgJob"></param>
        public PXFileInfo(string fileName, DataPackageJobInfo dataPkgJob) : base(fileName, dataPkgJob)
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
