using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;

namespace AnalysisManagerBase.OfflineJobs
{
    /// <summary>
    /// XML Utilities
    /// </summary>
    public static class XMLUtils
    {
        /// <summary>
        /// Extract the string value inside an XML element
        /// </summary>
        /// <param name="elementList">List of XML elements</param>
        /// <param name="elementName">Element name</param>
        /// <param name="valueIfMissing">Value to return, if the element name is not found</param>
        /// <returns>String value, or valueIfMissing if a parse error</returns>
        public static string GetXmlValue(IEnumerable<XElement> elementList, string elementName, string valueIfMissing = "")
        {
            var elements = elementList.Elements(elementName).ToList();

            if (elements.Count == 0)
                return valueIfMissing;

            var firstElement = elements[0];

            if (string.IsNullOrEmpty(firstElement?.Value))
                return valueIfMissing;

            return firstElement.Value;
        }

        /// <summary>
        /// Extract a date value inside an XML element
        /// </summary>
        /// <param name="elementList">List of XML elements</param>
        /// <param name="elementName">Element name</param>
        /// <param name="valueIfMissing">Date to return, if the element name is not found</param>
        /// <returns>Date, or valueIfMissing if a parse error</returns>
        public static DateTime GetXmlValue(IEnumerable<XElement> elementList, string elementName, DateTime valueIfMissing)
        {
            var valueText = GetXmlValue(elementList, elementName);

            if (string.IsNullOrWhiteSpace(valueText))
            {
                return valueIfMissing;
            }

            if (DateTime.TryParse(valueText, out var value))
            {
                return value;
            }

            return valueIfMissing;
        }

        /// <summary>
        /// Extract an integer value inside an XML element
        /// </summary>
        /// <param name="elementList">List of XML elements</param>
        /// <param name="elementName">Element name</param>
        /// <param name="valueIfMissing">Value to return, if the element name is not found</param>
        /// <returns>Value, or valueIfMissing if a parse error</returns>
        public static int GetXmlValue(IEnumerable<XElement> elementList, string elementName, int valueIfMissing)
        {
            var valueText = GetXmlValue(elementList, elementName);

            if (string.IsNullOrWhiteSpace(valueText))
            {
                return valueIfMissing;
            }

            if (int.TryParse(valueText, out var value))
            {
                return value;
            }

            return valueIfMissing;
        }

        /// <summary>
        /// Extract a float value inside an XML element
        /// </summary>
        /// <param name="elementList">List of XML elements</param>
        /// <param name="elementName">Element name</param>
        /// <param name="valueIfMissing">Value to return, if the element name is not found</param>
        /// <returns>Value, or valueIfMissing if a parse error</returns>
        public static float GetXmlValue(IEnumerable<XElement> elementList, string elementName, float valueIfMissing)
        {
            var valueText = GetXmlValue(elementList, elementName);

            if (string.IsNullOrWhiteSpace(valueText))
            {
                return valueIfMissing;
            }

            if (float.TryParse(valueText, out var value))
            {
                return value;
            }

            return valueIfMissing;
        }
    }
}
