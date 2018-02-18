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

namespace AnalysisManagerBase
{
    /// <summary>
    /// Writes a formatted XML file
    /// </summary>
    public class clsFormattedXMLWriter
    {

        #region "Properties"

        /// <summary>
        /// Error message
        /// </summary>
        public string ErrMsg { get; private set; }

        #endregion

        #region "Methods"

        /// <summary>
        /// Write XML to disk
        /// </summary>
        /// <param name="strXMLText"></param>
        /// <param name="outputFilePath"></param>
        /// <returns></returns>
        public bool WriteXMLToFile(string strXMLText, string outputFilePath)
        {

            XmlDocument objXMLDoc;

            ErrMsg = "";

            try
            {
                // Instantiate objXMLDoc
                objXMLDoc = new XmlDocument();
                objXMLDoc.LoadXml(strXMLText);

            }
            catch (Exception ex)
            {
                ErrMsg = "Error parsing the source XML text: " + ex.Message;
                return false;
            }

            try
            {
                // Write the XML to disk
                using (var xWriter = new XmlTextWriter(
                    new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read), System.Text.Encoding.UTF8))
                {
                    xWriter.Formatting = Formatting.Indented;
                    xWriter.Indentation = 2;
                    xWriter.IndentChar = ' ';

                    // Write out the XML
                    objXMLDoc.WriteTo(xWriter);
                }

                return true;

            }
            catch (Exception ex)
            {
                ErrMsg = "Error writing XML to file " + outputFilePath + ": " + ex.Message;
                return false;
            }

        }
        #endregion

    }
}
