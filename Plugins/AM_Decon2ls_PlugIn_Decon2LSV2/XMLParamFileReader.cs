using System.Collections.Generic;
using System.Linq;

namespace AnalysisManagerDecon2lsV2PlugIn
{
    /// <summary>
    /// XML Parameter file reader
    /// </summary>
    /// <remarks>This class is used by AnalysisToolRunnerDecon2ls</remarks>
    public class XMLParamFileReader
    {
        /// <summary>
        /// Sections and parameters
        /// </summary>
        private readonly Dictionary<string, Dictionary<string, string>> mSections;

        /// <summary>
        /// Parameter file path
        /// </summary>
        public string ParamFilePath { get; }

        /// <summary>
        /// Count of the number of parameters across all sections
        /// </summary>
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

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="paramFilePath"></param>
        public XMLParamFileReader(string paramFilePath)
        {
            ParamFilePath = paramFilePath;

            if (!System.IO.File.Exists(paramFilePath))
            {
                throw new System.IO.FileNotFoundException(paramFilePath);
            }

            mSections = CacheXMLParamFile(paramFilePath);
        }

        /// <summary>
        /// Parse an XML parameter file with the hierarchy of Section, ParamName, ParamValue
        /// </summary>
        /// <param name="paramFilePath"></param>
        /// <returns>Dictionary object where keys are section names and values are dictionary objects of key/value pairs</returns>
        private Dictionary<string, Dictionary<string, string>> CacheXMLParamFile(string paramFilePath)
        {
            var sections = new Dictionary<string, Dictionary<string, string>>();

            // Read the entire XML file into a Linq to XML XDocument object
            // Note: For this to work, the project must have a reference to System.XML.Linq
            var xParamFile = System.Xml.Linq.XDocument.Load(paramFilePath);

            var parameters = xParamFile.Elements();

            // Store the parameters
            CacheXMLParseSection(parameters, ref sections);

            return sections;
        }

        /// <summary>
        /// Parses the XML elements in parameters, populating parameters
        /// </summary>
        /// <param name="parameters">XML parameters to examine</param>
        /// <param name="parameterDictionary">Dictionary object where keys are section names and values are dictionary objects of key/value pairs</param>
        private void CacheXMLParseSection(IEnumerable<System.Xml.Linq.XElement> parameters,
                                            ref Dictionary<string, Dictionary<string, string>> parameterDictionary)
        {
            foreach (var parameter in parameters)
            {
                var descendants = parameter.Descendants().ToList();

                if (descendants.Count > 0)
                {
                    // Recursively call this function with the content
                    CacheXMLParseSection(descendants, ref parameterDictionary);
                }
                else
                {
                    if (parameter.Parent == null)
                        continue;

                    // Store this as a parameter
                    var section = parameter.Parent.Name.LocalName;
                    var paramName = parameter.Name.LocalName;
                    var paramValue = parameter.Value;

                    if (!parameterDictionary.TryGetValue(section, out var sectionSettings))
                    {
                        sectionSettings = new Dictionary<string, string>();
                        parameterDictionary.Add(section, sectionSettings);
                    }

                    if (!sectionSettings.ContainsKey(paramName))
                    {
                        sectionSettings.Add(paramName, paramValue);
                    }
                }
            }
        }

        /// <summary>
        /// Retrieve a parameter
        /// </summary>
        /// <param name="parameterName"></param>
        /// <param name="valueIfMissing"></param>
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

        /// <summary>
        /// Retrieve a parameter (from any section)
        /// </summary>
        /// <param name="parameterName"></param>
        /// <param name="valueIfMissing"></param>
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
    }
}