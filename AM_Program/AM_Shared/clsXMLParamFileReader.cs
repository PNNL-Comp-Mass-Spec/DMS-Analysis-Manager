
using System;
using System.Collections.Generic;
using System.Linq;

namespace AnalysisManagerBase
{

    [Obsolete("Unused")]
    public class clsXMLParamFileReader
    {

        protected string mParamFilePath;

        protected Dictionary<string, Dictionary<string, string>> mSections;

        public string ParamFilePath => mParamFilePath;

        public int ParameterCount
        {
            get
            {
                var intCount = 0;

                foreach (var section in mSections)
                {
                    intCount += section.Value.Count;
                }

                return intCount;
            }
        }

        public int SectionCount => mSections.Count;

        public clsXMLParamFileReader(string strParamFilePath)
        {
            mParamFilePath = strParamFilePath;

            if (!System.IO.File.Exists(strParamFilePath))
            {
                throw new System.IO.FileNotFoundException(strParamFilePath);
            }

            mSections = CacheXMLParamFile(mParamFilePath);
        }

        /// <summary>
        /// Parse an XML parameter file with the hierarchy of Section, ParamName, ParamValue 
        /// </summary>
        /// <param name="strParamFilePath"></param>
        /// <returns>Dictionary object where keys are section names and values are dictionary objects of key/value pairs</returns>
        /// <remarks></remarks>
        protected Dictionary<string, Dictionary<string, string>> CacheXMLParamFile(string strParamFilePath)
        {

            var dctSections = new Dictionary<string, Dictionary<string, string>>();

            // Read the entire XML file into a Linq to XML XDocument object
            // Note: For this to work, the project must have a reference to System.XML.Linq
            var xParamFile = System.Xml.Linq.XDocument.Load(strParamFilePath);

            var parameters = xParamFile.Elements();

            // Store the parameters
            CacheXMLParseSection(parameters, ref dctSections);

            return dctSections;

        }

        /// <summary>
        /// Parses the XML elements in parameters, populating dctParameters
        /// </summary>
        /// <param name="parameters">XML parameters to examine</param>
        /// <param name="dctParameters">Dictionary object where keys are section names and values are dictionary objects of key/value pairs</param>
        /// <remarks></remarks>
        protected void CacheXMLParseSection(IEnumerable<System.Xml.Linq.XElement> parameters,
                                            ref Dictionary<string, Dictionary<string, string>> dctParameters)
        {
            foreach (var parameter in parameters)
            {
                var descendants = parameter.Descendants().ToList();

                if (descendants.Count > 0)
                {
                    // Recursively call this function with the content
                    CacheXMLParseSection(descendants, ref dctParameters);
                }
                else
                {
                    if (parameter.Parent == null)
                        continue;

                    // Store this as a parameter
                    var strSection = parameter.Parent.Name.LocalName;
                    var strParamName = parameter.Name.LocalName;
                    var strParamValue = parameter.Value;

                    Dictionary<string, string> dctSectionSettings;

                    if (!dctParameters.TryGetValue(strSection, out dctSectionSettings))
                    {
                        dctSectionSettings = new Dictionary<string, string>();
                        dctParameters.Add(strSection, dctSectionSettings);
                    }

                    if (!dctSectionSettings.ContainsKey(strParamName))
                    {
                        dctSectionSettings.Add(strParamName, strParamValue);
                    }
                }
            }

        }

        public bool GetParameter(string strParameterName, bool blnValueIfMissing)
        {

            var strValue = GetParameter(strParameterName, string.Empty);

            if (string.IsNullOrEmpty(strValue))
                return blnValueIfMissing;

            bool blnValue;
            if (bool.TryParse(strValue, out blnValue))
            {
                return blnValue;
            }

            return blnValueIfMissing;

        }

        public string GetParameter(string strParameterName, string strValueIfMissing)
        {

            foreach (var section in mSections)
            {
                string strValue;

                if (section.Value.TryGetValue(strParameterName, out strValue))
                {
                    return strValue;
                }

            }

            return strValueIfMissing;

        }

        public string GetParameterBySection(string strSectionName, string strParameterName, string strValueIfMissing)
        {

            Dictionary<string, string> dctParameters;

            if (mSections.TryGetValue(strSectionName, out dctParameters))
            {
                string strValue;

                if (dctParameters.TryGetValue(strParameterName, out strValue))
                {
                    return strValue;
                }
            }

            return strValueIfMissing;

        }
    }
}