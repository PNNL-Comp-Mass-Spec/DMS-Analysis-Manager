namespace AnalysisManagerTopPICPlugIn
{
    internal class TopPICResultFileInfo
    {
        public string PrsmFileSuffix { get; }

        public string ProteoformFileSuffix { get; }

        public bool IsCsvDelimited { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="prsmFileSuffix"></param>
        /// <param name="proteoformFileSuffix"></param>
        /// <param name="isCsvDelimited"></param>
        public TopPICResultFileInfo(string prsmFileSuffix, string proteoformFileSuffix, bool isCsvDelimited = false)
        {
            PrsmFileSuffix = prsmFileSuffix;
            ProteoformFileSuffix = proteoformFileSuffix;
            IsCsvDelimited = isCsvDelimited;
        }
    }
}
