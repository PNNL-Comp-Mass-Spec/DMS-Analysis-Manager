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
            XmlTextWriter swOutfile;

            var success = false;

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
                // Initialize the XML writer
                swOutfile = new XmlTextWriter(new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read), System.Text.Encoding.UTF8)
                {
                    Formatting = Formatting.Indented,
                    Indentation = 2,
                    IndentChar = ' '
                };

            }
            catch (Exception ex)
            {
                ErrMsg = "Error opening the output file (" + outputFilePath + ") in WriteXMLToFile: " + ex.Message;
                return false;
            }

            try
            {
                // Write out the XML
                objXMLDoc.WriteTo(swOutfile);
                swOutfile.Close();

                success = true;

            }
            catch (Exception ex)
            {
                ErrMsg = "Error in WritePepXMLFile: " + ex.Message;
                swOutfile.Close();
            }

            return success;

        }
        #endregion

    }
}
