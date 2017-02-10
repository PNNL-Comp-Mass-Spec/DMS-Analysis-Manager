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

        #region "Module variables"
        #endregion
        string m_ErrMsg;

        #region "Properties"
        public string ErrMsg => m_ErrMsg;

        #endregion

        #region "Methods"
        public bool WriteXMLToFile(string strXMLText, string strOutputFilePath)
        {

            XmlDocument objXMLDoc;
            XmlTextWriter swOutfile;

            var blnSuccess = false;

            m_ErrMsg = "";

            try
            {
                // Instantiate objXMLDoc
                objXMLDoc = new XmlDocument();
                objXMLDoc.LoadXml(strXMLText);

            }
            catch (Exception ex)
            {
                m_ErrMsg = "Error parsing the source XML text: " + ex.Message;
                return false;
            }

            try
            {
                // Initialize the XML writer
                swOutfile = new XmlTextWriter(new FileStream(strOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read), System.Text.Encoding.UTF8)
                {
                    Formatting = Formatting.Indented,
                    Indentation = 2,
                    IndentChar = ' '
                };

            }
            catch (Exception ex)
            {
                m_ErrMsg = "Error opening the output file (" + strOutputFilePath + ") in WriteXMLToFile: " + ex.Message;
                return false;
            }

            try
            {
                // Write out the XML
                objXMLDoc.WriteTo(swOutfile);
                swOutfile.Close();

                blnSuccess = true;

            }
            catch (Exception ex)
            {
                m_ErrMsg = "Error in WritePepXMLFile: " + ex.Message;
                swOutfile.Close();
            }

            return blnSuccess;

        }
        #endregion

    }
}
