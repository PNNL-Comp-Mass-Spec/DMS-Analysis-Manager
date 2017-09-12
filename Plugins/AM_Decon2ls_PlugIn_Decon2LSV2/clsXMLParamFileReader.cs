
using System;
using System.Collections.Generic;
using System.Linq;

namespace AnalysisManagerBase
{

    /// <summary>
    /// XML Parameter file reader
    /// </summary>
    /// <remarks>This class is used by clsAnalysisToolRunnerDecon2ls</remarks>
    public class clsXMLParamFileReader
    {

        protected string mParamFilePath;

        protected Dictionary<string, Dictionary<string, string>> mSections;

        public string ParamFilePath => mParamFilePath;

        public int ParameterCount
        {
            get
            {
                var count = 0;

                foreach (var section in mSections)
                {
                    count += section.Value.Count;
                }

                return count;
            }
        }

        public int SectionCount => mSections.Count;

        public clsXMLParamFileReader(string paramFilePath)
        {
            mParamFilePath = paramFilePath;

            if (!System.IO.File.Exists(paramFilePath))
            {
                throw new System.IO.FileNotFoundException(paramFilePath);
            }

            mSections = CacheXMLParamFile(mParamFilePath);
        }

        /// <summary>
        /// Parse an XML parameter file with the hierarchy of Section, ParamName, ParamValue 
        /// </summary>
        /// <param name="paramFilePath"></param>
        /// <returns>Dictionary object where keys are section names and values are dictionary objects of key/value pairs</returns>
        /// <remarks></remarks>
        protected Dictionary<string, Dictionary<string, string>> CacheXMLParamFile(string paramFilePath)
        {

            var dctSections = new Dictionary<string, Dictionary<string, string>>();

            // Read the entire XML file into a Linq to XML XDocument object
            // Note: For this to work, the project must have a reference to System.XML.Linq
            var xParamFile = System.Xml.Linq.XDocument.Load(paramFilePath);

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
                    var section = parameter.Parent.Name.LocalName;
                    var paramName = parameter.Name.LocalName;
                    var paramValue = parameter.Value;


                    if (!dctParameters.TryGetValue(section, out var dctSectionSettings))
                    {
                        dctSectionSettings = new Dictionary<string, string>();
                        dctParameters.Add(section, dctSectionSettings);
                    }

                    if (!dctSectionSettings.ContainsKey(paramName))
                    {
                        dctSectionSettings.Add(paramName, paramValue);
                    }
                }
            }

        }

        public bool GetParameter(string parameterName, bool valueIfMissing)
        {

            var value = GetParameter(parameterName, string.Empty);

            if (string.IsNullOrEmpty(value))
                return valueIfMissing;

            if (bool.TryParse(value, out var boolValue))
            {
                return boolValue;
            }

            return valueIfMissing;

        }

        public string GetParameter(string parameterName, string valueIfMissing)
        {

            foreach (var section in mSections)
            {

                if (section.Value.TryGetValue(parameterName, out var value))
                {
                    return value;
                }

            }

            return valueIfMissing;

        }

        public string GetParameterBySection(string sectionName, string parameterName, string valueIfMissing)
        {


            if (mSections.TryGetValue(sectionName, out var dctParameters))
            {

                if (dctParameters.TryGetValue(parameterName, out var value))
                {
                    return value;
                }
            }

            return valueIfMissing;

        }
    }
}