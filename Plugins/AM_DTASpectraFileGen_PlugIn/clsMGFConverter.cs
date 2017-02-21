using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Xml;
using AnalysisManagerBase;
using PRISM;

namespace DTASpectraFileGen
{
    public class clsMGFConverter : clsEventNotifier
    {
        #region "Structures"

        private struct udtScanInfoType
        {
            public int ScanStart;
            public int ScanEnd;
            public int Charge;
        }

        #endregion

        #region "Member variables"

        private int m_DebugLevel;
        private string m_WorkDir;
        private string m_ErrMsg;

        private MascotGenericFileToDTA.clsMGFtoDTA mMGFtoDTA;

        #endregion

        #region "Properties"

        public string ErrorMessage
        {
            get
            {
                if (string.IsNullOrEmpty(m_ErrMsg))
                {
                    return string.Empty;
                }
                else
                {
                    return m_ErrMsg;
                }
            }
        }

        /// <summary>
        /// When true, the parent ion line will include text like "scan=5823 cs=3"
        /// </summary>
        /// <returns></returns>
        public bool IncludeExtraInfoOnParentIonLine { get; set; }

        /// <summary>
        /// If non-zero, spectra with fewer than this many ions are excluded from the _dta.txt file
        /// </summary>
        /// <returns></returns>
        public int MinimumIonsPerSpectrum { get; set; }

        public int SpectraCountWritten
        {
            get
            {
                if (mMGFtoDTA == null)
                {
                    return 0;
                }
                else
                {
                    return mMGFtoDTA.SpectraCountWritten;
                }
            }
        }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="intDebugLevel"></param>
        /// <param name="strWorkDir"></param>
        public clsMGFConverter(int intDebugLevel, string strWorkDir)
        {
            m_DebugLevel = intDebugLevel;
            m_WorkDir = strWorkDir;
            m_ErrMsg = string.Empty;
        }

        /// <summary>
        /// Convert .mgf file to _DTA.txt using MascotGenericFileToDTA.dll
        /// This functon is called by MakeDTAFilesThreaded
        /// </summary>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        public bool ConvertMGFtoDTA(clsAnalysisResources.eRawDataTypeConstants eRawDataType, string strDatasetName)
        {
            string strMGFFilePath = null;
            bool blnSuccess = false;

            m_ErrMsg = string.Empty;

            if (m_DebugLevel > 0)
            {
                OnDebugEvent("Converting .MGF file to _DTA.txt");
            }

            strMGFFilePath = Path.Combine(m_WorkDir, strDatasetName + clsAnalysisResources.DOT_MGF_EXTENSION);

            if (eRawDataType == clsAnalysisResources.eRawDataTypeConstants.mzML)
            {
                // Read the .mzML file to construct a mapping between "title" line and scan number
                // If necessary, update the .mgf file to have new "title" lines that clsMGFtoDTA will recognize

                var strMzMLFilePath = Path.Combine(m_WorkDir, strDatasetName + clsAnalysisResources.DOT_MZML_EXTENSION);

                blnSuccess = UpdateMGFFileTitleLinesUsingMzML(strMzMLFilePath, strMGFFilePath, strDatasetName);
                if (!blnSuccess)
                {
                    return false;
                }
            }

            mMGFtoDTA = new MascotGenericFileToDTA.clsMGFtoDTA
            {
                CreateIndividualDTAFiles = false,
                FilterSpectra = false,
                ForceChargeAddnForPredefined2PlusOr3Plus = false,
                GuesstimateChargeForAllSpectra = false,
                IncludeExtraInfoOnParentIonLine = IncludeExtraInfoOnParentIonLine,
                LogMessagesToFile = false,
                MinimumIonsPerSpectrum = MinimumIonsPerSpectrum,
                MaximumIonsPerSpectrum = 0
            };
            mMGFtoDTA.ErrorEvent += mMGFtoDTA_ErrorEvent;

            blnSuccess = mMGFtoDTA.ProcessFile(strMGFFilePath, m_WorkDir);

            if (!blnSuccess && string.IsNullOrEmpty(m_ErrMsg))
            {
                m_ErrMsg = mMGFtoDTA.GetErrorMessage();
            }

            return blnSuccess;
        }

        private Dictionary<string, string> GetCVParams(XmlTextReader objXMLReader, string strCurrentElementName)
        {
            var lstCVParams = new Dictionary<string, string>();
            string strAccession = null;
            string strValue = null;

            while (objXMLReader.Read())
            {
                XMLTextReaderSkipWhitespace(objXMLReader);

                if (objXMLReader.NodeType == XmlNodeType.EndElement & objXMLReader.Name == strCurrentElementName)
                {
                    break;
                }

                if (objXMLReader.NodeType == XmlNodeType.Element && objXMLReader.Name == "cvParam")
                {
                    strAccession = XMLTextReaderGetAttributeValue(objXMLReader, "accession", string.Empty);
                    strValue = XMLTextReaderGetAttributeValue(objXMLReader, "value", string.Empty);

                    if (!lstCVParams.ContainsKey(strAccession))
                    {
                        lstCVParams.Add(strAccession, strValue);
                    }
                }
            }
            return lstCVParams;
        }

        private bool ParseMzMLFile(string strMzMLFilePath, out bool blnAutoNumberScans, Dictionary<string, udtScanInfoType> lstSpectrumIDToScanNumber)
        {
            string strSpectrumID = string.Empty;

            int intScanNumberStart = 0;
            int intScanNumberEnd = 0;
            int intCharge = 0;

            var intScanNumberCurrent = 0;
            string strValue = string.Empty;
            int intValue = 0;

            blnAutoNumberScans = false;

            if (lstSpectrumIDToScanNumber == null)
            {
                lstSpectrumIDToScanNumber = new Dictionary<string, udtScanInfoType>();
            }

            using (var objXMLReader = new XmlTextReader(strMzMLFilePath))
            {
                while (objXMLReader.Read())
                {
                    XMLTextReaderSkipWhitespace(objXMLReader);
                    if (!(objXMLReader.ReadState == ReadState.Interactive))
                        break;

                    udtScanInfoType udtScanInfo = new udtScanInfoType();
                    if (objXMLReader.NodeType == XmlNodeType.Element)
                    {
                        switch (objXMLReader.Name)
                        {
                            case "spectrum":

                                strSpectrumID = XMLTextReaderGetAttributeValue(objXMLReader, "id", string.Empty);

                                if (!string.IsNullOrEmpty(strSpectrumID))
                                {
                                    if (MsMsDataFileReader.clsMsMsDataFileReaderBaseClass.ExtractScanInfoFromDtaHeader(strSpectrumID,
                                        out intScanNumberStart, out intScanNumberEnd, out intCharge))
                                    {
                                        // This title is in a standard format
                                        udtScanInfo.ScanStart = intScanNumberStart;
                                        udtScanInfo.ScanEnd = intScanNumberEnd;
                                        udtScanInfo.Charge = intCharge;
                                    }
                                    else
                                    {
                                        blnAutoNumberScans = true;
                                    }

                                    if (blnAutoNumberScans)
                                    {
                                        intScanNumberCurrent += 1;
                                        udtScanInfo.ScanStart = intScanNumberCurrent;
                                        udtScanInfo.ScanEnd = intScanNumberCurrent;
                                        // Store a charge of 0 for now; we'll update it later if the selectedIon element has a MS:1000041 attribute
                                        udtScanInfo.Charge = 0;
                                    }
                                    else
                                    {
                                        intScanNumberCurrent = intScanNumberStart;
                                    }
                                }

                                break;

                            case "selectedIon":
                                // Read the cvParams for this selected ion
                                var lstCVParams = GetCVParams(objXMLReader, "selectedIon");

                                if (lstCVParams.TryGetValue("MS:1000041", out strValue))
                                {
                                    if (int.TryParse(strValue, out intValue))
                                    {
                                        udtScanInfo.Charge = intValue;
                                    }
                                }
                                break;
                        }
                    }
                    else if (objXMLReader.NodeType == XmlNodeType.EndElement && objXMLReader.Name == "spectrum")
                    {
                        // Store this spectrum
                        if (!string.IsNullOrEmpty(strSpectrumID))
                        {
                            lstSpectrumIDToScanNumber.Add(strSpectrumID, udtScanInfo);
                        }
                    }
                }
            }

            return true;
        }

        private string XMLTextReaderGetAttributeValue(XmlTextReader objXMLReader, string strAttributeName, string strValueIfMissing)
        {
            objXMLReader.MoveToAttribute(strAttributeName);
            if (objXMLReader.ReadAttributeValue())
            {
                return objXMLReader.Value;
            }
            else
            {
                return string.Copy(strValueIfMissing);
            }
        }

        private string XMLTextReaderGetInnerText(XmlTextReader objXMLReader)
        {
            string strValue = string.Empty;
            bool blnSuccess = false;

            if (objXMLReader.NodeType == XmlNodeType.Element)
            {
                // Advance the reader so that we can read the value
                blnSuccess = objXMLReader.Read();
            }
            else
            {
                blnSuccess = true;
            }

            if (blnSuccess && !(objXMLReader.NodeType == XmlNodeType.Whitespace) & objXMLReader.HasValue)
            {
                strValue = objXMLReader.Value;
            }

            return strValue;
        }

        private void XMLTextReaderSkipWhitespace(XmlTextReader objXMLReader)
        {
            if (objXMLReader.NodeType == XmlNodeType.Whitespace)
            {
                // Whitspace; read the next node
                objXMLReader.Read();
            }
        }

        private bool UpdateMGFFileTitleLinesUsingMzML(string strMzMLFilePath, string strMGFFilePath, string strDatasetName)
        {
            string strNewMGFFile = null;
            string strLineIn = null;
            string strTitle = null;

            bool blnSuccess = false;
            bool blnAutoNumberScans = false;

            var lstSpectrumIDtoScanNumber = new Dictionary<string, udtScanInfoType>();

            try
            {
                // Open the mzXML file and look for "spectrum" elements with an "id" attribute
                // Also look for the charge state

                if (m_DebugLevel >= 1)
                {
                    OnDebugEvent(
                        "Parsing the .mzML file to create the spectrum ID to scan number mapping");
                }

                blnSuccess = ParseMzMLFile(strMzMLFilePath, out blnAutoNumberScans, lstSpectrumIDtoScanNumber);

                if (!blnSuccess)
                {
                    OnErrorEvent("ParseMzMLFile returned false; aborting");
                    return false;
                }

                if (!blnAutoNumberScans)
                {
                    // Nothing to update; exit this function
                    OnStatusEvent(
                        "Spectrum IDs in the mzML file were in the format StartScan.EndScan.Charge; no need to update the MGF file");
                    return true;
                }

                if (m_DebugLevel >= 1)
                {
                    OnDebugEvent("Updating the Title lines in the MGF file");
                }

                strNewMGFFile = Path.GetTempFileName();

                // Now read the MGF file and update the title lines
                using (var srSourceMGF = new StreamReader(new FileStream(strMGFFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                using (var swNewMGF = new StreamWriter(new FileStream(strNewMGFFile, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (!srSourceMGF.EndOfStream)
                    {
                        strLineIn = srSourceMGF.ReadLine();

                        if (string.IsNullOrEmpty(strLineIn))
                        {
                            strLineIn = string.Empty;
                        }
                        else
                        {
                            if (strLineIn.StartsWith("TITLE="))
                            {
                                strTitle = strLineIn.Substring("TITLE=".Length);

                                // Look for strTitle in lstSpectrumIDtoScanNumber
                                udtScanInfoType udtScanInfo;
                                if (lstSpectrumIDtoScanNumber.TryGetValue(strTitle, out udtScanInfo))
                                {
                                    strLineIn = "TITLE=" + strDatasetName + "." + udtScanInfo.ScanStart.ToString("0000") + "." +
                                                udtScanInfo.ScanEnd.ToString("0000") + ".";
                                    if (udtScanInfo.Charge > 0)
                                    {
                                        // Also append charge
                                        strLineIn += udtScanInfo.Charge;
                                    }
                                }
                            }
                        }

                        swNewMGF.WriteLine(strLineIn);
                    }
                }

                if (m_DebugLevel >= 1)
                {
                    OnDebugEvent(
                        "Update complete; replacing the original .MGF file");
                }

                // Delete the original .mgf file and replace it with strNewMGFFile
                PRISM.clsProgRunner.GarbageCollectNow();
                Thread.Sleep(500);
                clsAnalysisToolRunnerBase.DeleteFileWithRetries(strMGFFilePath, m_DebugLevel);
                Thread.Sleep(500);

                var ioNewMGF = new FileInfo(strNewMGFFile);
                ioNewMGF.MoveTo(strMGFFilePath);

                blnSuccess = true;
            }
            catch (Exception ex)
            {
                m_ErrMsg = "Error updating the MGF file title lines using the .mzML file";
                OnErrorEvent(m_ErrMsg + ": " + ex.Message);
                blnSuccess = false;
            }

            return blnSuccess;
        }

        private void mMGFtoDTA_ErrorEvent(string strMessage)
        {
            OnErrorEvent(strMessage);

            if (string.IsNullOrEmpty(m_ErrMsg))
            {
                m_ErrMsg = "MGFtoDTA_Error: " + strMessage;
            }
            else if (m_ErrMsg.Length < 300)
            {
                m_ErrMsg += "; MGFtoDTA_Error: " + strMessage;
            }
        }
    }
}
