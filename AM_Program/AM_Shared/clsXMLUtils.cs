using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;

namespace AnalysisManagerBase
{
    public static class clsXMLUtils
    {

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
