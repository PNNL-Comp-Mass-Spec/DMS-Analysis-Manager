namespace AnalysisManagerTopPICPlugIn
{
    internal class TopPICResultFileInfo
    {
        public string BaseName { get; }

        public string PrsmFileSuffix { get; }

        public string ProteoformFileSuffix { get; }

        public bool IsCsvDelimited { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="baseName"></param>
        /// <param name="prsmFileSuffix"></param>
        /// <param name="proteoformFileSuffix"></param>
        /// <param name="isCsvDelimited"></param>
        public TopPICResultFileInfo(string baseName, string prsmFileSuffix, string proteoformFileSuffix, bool isCsvDelimited = false)
        {
            BaseName = baseName;
            PrsmFileSuffix = prsmFileSuffix;
            ProteoformFileSuffix = proteoformFileSuffix;
            IsCsvDelimited = isCsvDelimited;
        }
    }
}
