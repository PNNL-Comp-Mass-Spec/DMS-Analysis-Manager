using System;
using System.Collections.Generic;

namespace AnalysisManagerPRIDEConverterPlugIn
{
    public class clsSampleMetadata
    {
        public struct udtCvParamInfoType
        {
            public string Accession;
            public string CvRef;
            public string Value;
            public string Name;
            public string unitCvRef;
            public string unitName;
            public string unitAccession;

            public void Clear()
            {
                Accession = string.Empty;
                CvRef = string.Empty;
                Value = string.Empty;
                Name = string.Empty;
                unitCvRef = string.Empty;
                unitName = string.Empty;
                unitAccession = string.Empty;
            }
        }

        public string Species { get; set; }                                         // Recommended to use NEWT CVs
        public string Tissue { get; set; }                                          // Recommended to use BRENDA CVs (BTO)
        public string CellType { get; set; }                                        // Recommended to use CL CVs
        public string Disease { get; set; }                                         // Recommended to use DOID CVs
        public Dictionary<string, udtCvParamInfoType> Modifications { get; set; }   // Recommended to use PSI-MOD, though Unimod is acceptable
        public string InstrumentGroup { get; set; }                                 // Recommended to use MS CVs
        public string Quantification { get; set; }
        public string ExperimentalFactor { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks></remarks>
        public clsSampleMetadata()
        {
            Clear();
        }

        public void Clear()
        {
            Species = string.Empty;
            Tissue = string.Empty;
            CellType = string.Empty;
            Disease = string.Empty;
            Modifications = new Dictionary<string, udtCvParamInfoType>(StringComparer.OrdinalIgnoreCase);
            InstrumentGroup = string.Empty;
            Quantification = string.Empty;
            ExperimentalFactor = string.Empty;
        }
    }
}
