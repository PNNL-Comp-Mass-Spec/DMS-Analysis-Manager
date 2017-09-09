using System.Collections.Generic;

namespace AnalysisManagerPRIDEConverterPlugIn
{
    /// <summary>
    /// Tracks information about a result file
    /// </summary>
    public class clsResultFileContainer
    {
        /// <summary>
        /// Tracks the .mgf or _dta.txt or .mzML file for the analysis job
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public string MGFFilePath { get; set; }

        /// <summary>
        /// One or more .mzid.gz files
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public List<string> MzIDFilePaths { get; set; }

        /// <summary>
        /// Tracks the .pepXML.gz file for the analysis job
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public string PepXMLFile { get; set; }

        /// <summary>
        /// PRIDE XML file path
        /// </summary>
        public string PrideXmlFilePath { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks></remarks>
        public clsResultFileContainer()
        {
            MzIDFilePaths = new List<string>();
        }
    }
}
