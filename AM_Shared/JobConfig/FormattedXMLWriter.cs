using System;
using System.IO;
using System.Xml;

//*********************************************************************************************************
// Written by Matt Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2008, Battelle Memorial Institute
// Created 09/23/2008
//
//*********************************************************************************************************

namespace AnalysisManagerBase.JobConfig
{
    /// <summary>
    /// Writes a formatted XML file
    /// </summary>
    public class FormattedXMLWriter
    {
        /// <summary>
        /// Error message
        /// </summary>
        public string ErrMsg { get; private set; }

        /// <summary>
        /// Write XML to disk
        /// </summary>
        /// <param name="xmlText">XML, as a string</param>
        /// <param name="outputFilePath">Output file path</param>
        public bool WriteXMLToFile(string xmlText, string outputFilePath)
        {
            XmlDocument doc;

            ErrMsg = "";

            try
            {
                // Instantiate doc
                doc = new XmlDocument();
                doc.LoadXml(xmlText);
            }
            catch (Exception ex)
            {
                ErrMsg = "Error parsing the source XML text: " + ex.Message;
                return false;
            }

            try
            {
                // Write the XML to disk
                using var xWriter = new XmlTextWriter(
                    new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read), System.Text.Encoding.UTF8)
                {
                    Formatting = Formatting.Indented,
                    Indentation = 2,
                    IndentChar = ' '
                };

                // Write out the XML
                doc.WriteTo(xWriter);

                return true;
            }
            catch (Exception ex)
            {
                ErrMsg = "Error writing XML to file " + outputFilePath + ": " + ex.Message;
                return false;
            }
        }
    }
}
