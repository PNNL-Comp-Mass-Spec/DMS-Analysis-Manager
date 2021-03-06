﻿using System.Collections.Generic;

namespace AnalysisManagerPRIDEConverterPlugIn
{
    /// <summary>
    /// Tracks information about a result file
    /// </summary>
    public class ResultFileContainer
    {
        /// <summary>
        /// Tracks the .mgf or _dta.txt or .mzML file for the analysis job
        /// </summary>
        public string MGFFilePath { get; set; }

        /// <summary>
        /// One or more .mzid.gz files
        /// </summary>
        public List<string> MzIDFilePaths { get; set; }

        /// <summary>
        /// Tracks the .pepXML.gz file for the analysis job
        /// </summary>
        public string PepXMLFile { get; set; }

        /// <summary>
        /// PRIDE XML file path
        /// </summary>
        public string PrideXmlFilePath { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public ResultFileContainer()
        {
            MzIDFilePaths = new List<string>();
        }
    }
}
