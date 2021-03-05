using System;
using System.Collections.Generic;

namespace AnalysisManagerPRIDEConverterPlugIn
{
    /// <summary>
    /// Sample metadata
    /// </summary>
    public class clsSampleMetadata
    {
        // Ignore Spelling: CvRef, UniMod

        /// <summary>
        /// CV Param Info
        /// </summary>
        public struct udtCvParamInfoType
        {
            /// <summary>
            /// Accession
            /// </summary>
            public string Accession;

            /// <summary>
            /// CV ref ID
            /// </summary>
            public string CvRef;

            /// <summary>
            /// Value
            /// </summary>
            public string Value;

            /// <summary>
            /// Human readable name
            /// </summary>
            public string Name;

            /// <summary>
            /// Units for CvRef
            /// </summary>
            public string unitCvRef;

            /// <summary>
            /// Units for Name
            /// </summary>
            public string unitName;

            /// <summary>
            /// Units for Accession
            /// </summary>
            public string unitAccession;

            /// <summary>
            /// Reset values to the default
            /// </summary>
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

        /// <summary>
        /// Species
        /// </summary>
        /// <remarks>Recommended to use NEWT CVs</remarks>
        public string Species { get; set; }

        /// <summary>
        /// Tissue
        /// </summary>
        /// <remarks>Recommended to use BRENDA CVs (BTO)</remarks>
        public string Tissue { get; set; }

        /// <summary>
        /// Cell type
        /// </summary>
        /// <remarks>Recommended to use CL CVs</remarks>
        public string CellType { get; set; }

        /// <summary>
        /// Disease
        /// </summary>
        /// <remarks>Recommended to use DOID CVs</remarks>
        public string Disease { get; set; }

        /// <summary>
        /// Modifications
        /// </summary>
        /// <remarks>Recommended to use PSI-MOD, though UniMod is acceptable</remarks>
        public Dictionary<string, udtCvParamInfoType> Modifications { get; set; }

        /// <summary>
        /// Instrument Group
        /// </summary>
        /// <remarks>Recommended to use MS CVs</remarks>
        public string InstrumentGroup { get; set; }

        /// <summary>
        /// Instrument Name
        /// </summary>
        /// <remarks>Recommended to use MS CVs</remarks>
        public string InstrumentName { get; set; }

        /// <summary>
        /// Quantification details
        /// </summary>
        public string Quantification { get; set; }

        /// <summary>
        /// Factor metadata describing the sample
        /// </summary>
        public string ExperimentalFactor { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public clsSampleMetadata()
        {
            Modifications = new Dictionary<string, udtCvParamInfoType>(StringComparer.OrdinalIgnoreCase);
            Clear();
        }

        /// <summary>
        /// Reset values to the default
        /// </summary>
        public void Clear()
        {
            Species = string.Empty;
            Tissue = string.Empty;
            CellType = string.Empty;
            Disease = string.Empty;
            Modifications.Clear();
            InstrumentGroup = string.Empty;
            InstrumentName = string.Empty;
            Quantification = string.Empty;
            ExperimentalFactor = string.Empty;
        }
    }
}
