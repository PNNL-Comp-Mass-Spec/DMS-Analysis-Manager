namespace AnalysisManagerTopPICPlugIn
{
    internal class TopPICResultFileInfo
    {
        // Ignore Spelling: proteoform, prsm

        public string BaseName { get; }

        public string PrsmFileSuffix { get; }

        public string ProteoformFileSuffix { get; }

        public bool IsCsvDelimited { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="baseName">Base name</param>
        /// <param name="prsmFileSuffix">PrSM file suffix</param>
        /// <param name="proteoformFileSuffix">Proteoform file suffix</param>
        /// <param name="isCsvDelimited">True if a csv file, false if a TSV file</param>
        public TopPICResultFileInfo(string baseName, string prsmFileSuffix, string proteoformFileSuffix, bool isCsvDelimited = false)
        {
            BaseName = baseName;
            PrsmFileSuffix = prsmFileSuffix;
            ProteoformFileSuffix = proteoformFileSuffix;
            IsCsvDelimited = isCsvDelimited;
        }

        /// <summary>
        /// Show the PrSM filename and the Proteoform filename
        /// </summary>
        public override string ToString()
        {
            return string.Format("PrSM file: {0}, Proteoform file: {1}", BaseName + PrsmFileSuffix, BaseName + ProteoformFileSuffix);
        }
    }
}
