using System;
using System.Collections.Generic;
using System.IO;
using System.Xml;
using AnalysisManagerBase;
using PRISM;

namespace DTASpectraFileGen
{
    public class clsMGFConverter : EventNotifier
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

        private readonly int mDebugLevel;
        private readonly string mWorkDir;
        private string mErrMsg;

        private MascotGenericFileToDTA.clsMGFtoDTA mMGFtoDTA;

        #endregion

        #region "Properties"

        public string ErrorMessage
        {
            get
            {
                if (string.IsNullOrEmpty(mErrMsg))
                {
                    return string.Empty;
                }

                return mErrMsg;
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

                return mMGFtoDTA.SpectraCountWritten;
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
            mDebugLevel = intDebugLevel;
            mWorkDir = strWorkDir;
            mErrMsg = string.Empty;
        }

        /// <summary>
        /// Convert .mgf file to _DTA.txt using MascotGenericFileToDTA.dll
        /// This function is called by MakeDTAFilesThreaded
        /// </summary>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        public bool ConvertMGFtoDTA(clsAnalysisResources.eRawDataTypeConstants eRawDataType, string strDatasetName)
        {
            bool blnSuccess;

            mErrMsg = string.Empty;

            if (mDebugLevel > 0)
            {
                OnDebugEvent("Converting .MGF file to _DTA.txt");
            }

            var strMGFFilePath = Path.Combine(mWorkDir, strDatasetName + clsAnalysisResources.DOT_MGF_EXTENSION);

            if (eRawDataType == clsAnalysisResources.eRawDataTypeConstants.mzML)
            {
                // Read the .mzML file to construct a mapping between "title" line and scan number
                // If necessary, update the .mgf file to have new "title" lines that clsMGFtoDTA will recognize

                var strMzMLFilePath = Path.Combine(mWorkDir, strDatasetName + clsAnalysisResources.DOT_MZML_EXTENSION);

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
            mMGFtoDTA.ErrorEvent += MGFtoDTA_ErrorEvent;

            blnSuccess = mMGFtoDTA.ProcessFile(strMGFFilePath, mWorkDir);

            if (!blnSuccess && string.IsNullOrEmpty(mErrMsg))
            {
                mErrMsg = mMGFtoDTA.GetErrorMessage();
            }

            return blnSuccess;
        }

        private Dictionary<string, string> GetCVParams(XmlReader objXMLReader, string strCurrentElementName)
        {
            var lstCVParams = new Dictionary<string, string>();

            while (objXMLReader.Read())
            {
                XMLTextReaderSkipWhitespace(objXMLReader);

                if (objXMLReader.NodeType == XmlNodeType.EndElement && objXMLReader.Name == strCurrentElementName)
                {
                    break;
                }

                if (objXMLReader.NodeType == XmlNodeType.Element && objXMLReader.Name == "cvParam")
                {
                    var strAccession = XMLTextReaderGetAttributeValue(objXMLReader, "accession", string.Empty);
                    var strValue = XMLTextReaderGetAttributeValue(objXMLReader, "value", string.Empty);

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
            var strSpectrumID = string.Empty;

            var intScanNumberCurrent = 0;

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
                    if (objXMLReader.ReadState != ReadState.Interactive)
                        break;

                    var udtScanInfo = new udtScanInfoType();
                    if (objXMLReader.NodeType == XmlNodeType.Element)
                    {
                        switch (objXMLReader.Name)
                        {
                            case "spectrum":

                                strSpectrumID = XMLTextReaderGetAttributeValue(objXMLReader, "id", string.Empty);

                                if (!string.IsNullOrEmpty(strSpectrumID))
                                {
                                    if (MsMsDataFileReader.clsMsMsDataFileReaderBaseClass.ExtractScanInfoFromDtaHeader(strSpectrumID,
                                        out var intScanNumberStart, out var intScanNumberEnd, out var intCharge))
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

                                if (lstCVParams.TryGetValue("MS:1000041", out var strValue))
                                {
                                    if (int.TryParse(strValue, out var intValue))
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

        private string XMLTextReaderGetAttributeValue(XmlReader objXMLReader, string strAttributeName, string strValueIfMissing)
        {
            objXMLReader.MoveToAttribute(strAttributeName);
            if (objXMLReader.ReadAttributeValue())
            {
                return objXMLReader.Value;
            }

            return string.Copy(strValueIfMissing);
        }

        [Obsolete("Unused")]
        private string XMLTextReaderGetInnerText(XmlReader objXMLReader)
        {
            var strValue = string.Empty;
            bool blnSuccess;

            if (objXMLReader.NodeType == XmlNodeType.Element)
            {
                // Advance the reader so that we can read the value
                blnSuccess = objXMLReader.Read();
            }
            else
            {
                blnSuccess = true;
            }

            if (blnSuccess && objXMLReader.NodeType != XmlNodeType.Whitespace && objXMLReader.HasValue)
            {
                strValue = objXMLReader.Value;
            }

            return strValue;
        }

        private void XMLTextReaderSkipWhitespace(XmlReader objXMLReader)
        {
            if (objXMLReader.NodeType == XmlNodeType.Whitespace)
            {
                // Whitspace; read the next node
                objXMLReader.Read();
            }
        }

        private bool UpdateMGFFileTitleLinesUsingMzML(string strMzMLFilePath, string strMGFFilePath, string strDatasetName)
        {
            var lstSpectrumIDtoScanNumber = new Dictionary<string, udtScanInfoType>();

            try
            {
                // Open the mzXML file and look for "spectrum" elements with an "id" attribute
                // Also look for the charge state

                if (mDebugLevel >= 1)
                {
                    OnDebugEvent(
                        "Parsing the .mzML file to create the spectrum ID to scan number mapping");
                }

                var success = ParseMzMLFile(strMzMLFilePath, out var blnAutoNumberScans, lstSpectrumIDtoScanNumber);

                if (!success)
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

                if (mDebugLevel >= 1)
                {
                    OnDebugEvent("Updating the Title lines in the MGF file");
                }

                var strNewMGFFile = Path.GetTempFileName();

                // Now read the MGF file and update the title lines
                using (var srSourceMGF = new StreamReader(new FileStream(strMGFFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                using (var swNewMGF = new StreamWriter(new FileStream(strNewMGFFile, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (!srSourceMGF.EndOfStream)
                    {
                        var strLineIn = srSourceMGF.ReadLine();

                        if (string.IsNullOrEmpty(strLineIn))
                        {
                            strLineIn = string.Empty;
                        }
                        else
                        {
                            if (strLineIn.StartsWith("TITLE="))
                            {
                                var strTitle = strLineIn.Substring("TITLE=".Length);

                                // Look for strTitle in lstSpectrumIDtoScanNumber
                                if (lstSpectrumIDtoScanNumber.TryGetValue(strTitle, out var udtScanInfo))
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

                if (mDebugLevel >= 1)
                {
                    OnDebugEvent(
                        "Update complete; replacing the original .MGF file");
                }

                // Delete the original .mgf file and replace it with strNewMGFFile
                ProgRunner.GarbageCollectNow();

                clsAnalysisToolRunnerBase.DeleteFileWithRetries(strMGFFilePath, mDebugLevel);

                var ioNewMGF = new FileInfo(strNewMGFFile);
                ioNewMGF.MoveTo(strMGFFilePath);

                return true;
            }
            catch (Exception ex)
            {
                mErrMsg = "Error updating the MGF file title lines using the .mzML file";
                OnErrorEvent(mErrMsg + ": " + ex.Message);
                return false;
            }

        }

        private void MGFtoDTA_ErrorEvent(string strMessage)
        {
            OnErrorEvent(strMessage);

            if (string.IsNullOrEmpty(mErrMsg))
            {
                mErrMsg = "MGFtoDTA_Error: " + strMessage;
            }
            else if (mErrMsg.Length < 300)
            {
                mErrMsg += "; MGFtoDTA_Error: " + strMessage;
            }
        }
    }
}
