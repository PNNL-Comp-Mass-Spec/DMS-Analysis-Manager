using System;
using System.Collections.Generic;
using System.IO;
using System.Xml;
using AnalysisManagerBase.AnalysisTool;
using PRISM;

namespace DTASpectraFileGen
{
    public class MGFConverter : EventNotifier
    {
        // Ignore Spelling: cvParam, IDtoScan, MGFtoDTA

        private struct ScanInfo
        {
            public int ScanStart;
            public int ScanEnd;
            public int Charge;
        }

        private readonly int mDebugLevel;
        private readonly string mWorkDir;
        private string mErrMsg;

        private MascotGenericFileToDTA.clsMGFtoDTA mMGFtoDTA;

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
        public bool IncludeExtraInfoOnParentIonLine { get; set; }

        /// <summary>
        /// If non-zero, spectra with fewer than this many ions are excluded from the _dta.txt file
        /// </summary>
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

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="debugLevel"></param>
        /// <param name="workDir"></param>
        public MGFConverter(int debugLevel, string workDir)
        {
            mDebugLevel = debugLevel;
            mWorkDir = workDir;
            mErrMsg = string.Empty;
        }

        /// <summary>
        /// Convert .mgf file to _DTA.txt using MascotGenericFileToDTA.dll
        /// this method is called by MakeDTAFilesThreaded
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        public bool ConvertMGFtoDTA(AnalysisResources.RawDataTypeConstants rawDataType, string datasetName)
        {
            bool success;

            mErrMsg = string.Empty;

            if (mDebugLevel > 0)
            {
                OnDebugEvent("Converting .MGF file to _DTA.txt");
            }

            var mgfFilePath = Path.Combine(mWorkDir, datasetName + AnalysisResources.DOT_MGF_EXTENSION);

            if (rawDataType == AnalysisResources.RawDataTypeConstants.mzML)
            {
                // Read the .mzML file to construct a mapping between "title" line and scan number
                // If necessary, update the .mgf file to have new "title" lines that MGFtoDTA will recognize

                var mzMLFilePath = Path.Combine(mWorkDir, datasetName + AnalysisResources.DOT_MZML_EXTENSION);

                success = UpdateMGFFileTitleLinesUsingMzML(mzMLFilePath, mgfFilePath, datasetName);

                if (!success)
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

            success = mMGFtoDTA.ProcessFile(mgfFilePath, mWorkDir);

            if (!success && string.IsNullOrEmpty(mErrMsg))
            {
                mErrMsg = mMGFtoDTA.GetErrorMessage();
            }

            return success;
        }

        private Dictionary<string, string> GetCVParams(XmlReader reader, string currentElementName)
        {
            var cvParams = new Dictionary<string, string>();

            while (reader.Read())
            {
                XMLTextReaderSkipWhitespace(reader);

                if (reader.NodeType == XmlNodeType.EndElement && reader.Name == currentElementName)
                {
                    break;
                }

                if (reader.NodeType == XmlNodeType.Element && reader.Name == "cvParam")
                {
                    var accession = XMLTextReaderGetAttributeValue(reader, "accession", string.Empty);
                    var value = XMLTextReaderGetAttributeValue(reader, "value", string.Empty);

                    if (!cvParams.ContainsKey(accession))
                    {
                        cvParams.Add(accession, value);
                    }
                }
            }
            return cvParams;
        }

        private bool ParseMzMLFile(string mzMLFilePath, out bool autoNumberScans, Dictionary<string, ScanInfo> spectrumIDToScanNumber)
        {
            var spectrumID = string.Empty;

            var scanNumberCurrent = 0;

            autoNumberScans = false;

            spectrumIDToScanNumber ??= new Dictionary<string, ScanInfo>();

            using var reader = new XmlTextReader(mzMLFilePath);

            while (reader.Read())
            {
                XMLTextReaderSkipWhitespace(reader);

                if (reader.ReadState != ReadState.Interactive)
                    break;

                var scanInfo = new ScanInfo();

                if (reader.NodeType == XmlNodeType.Element)
                {
                    switch (reader.Name)
                    {
                        case "spectrum":

                            spectrumID = XMLTextReaderGetAttributeValue(reader, "id", string.Empty);

                            if (!string.IsNullOrEmpty(spectrumID))
                            {
                                if (MsMsDataFileReader.clsMsMsDataFileReaderBaseClass.ExtractScanInfoFromDtaHeader(spectrumID,
                                    out var scanNumberStart, out var scanNumberEnd, out var charge))
                                {
                                    // This title is in a standard format
                                    scanInfo.ScanStart = scanNumberStart;
                                    scanInfo.ScanEnd = scanNumberEnd;
                                    scanInfo.Charge = charge;
                                }
                                else
                                {
                                    autoNumberScans = true;
                                }

                                if (autoNumberScans)
                                {
                                    scanNumberCurrent++;
                                    scanInfo.ScanStart = scanNumberCurrent;
                                    scanInfo.ScanEnd = scanNumberCurrent;
                                    // Store a charge of 0 for now; we'll update it later if the selectedIon element has a MS:1000041 attribute
                                    scanInfo.Charge = 0;
                                }
                                else
                                {
                                    scanNumberCurrent = scanNumberStart;
                                }
                            }

                            break;

                        case "selectedIon":
                            // Read the cvParams for this selected ion
                            var cvParams = GetCVParams(reader, "selectedIon");

                            if (cvParams.TryGetValue("MS:1000041", out var valueText))
                            {
                                if (int.TryParse(valueText, out var value))
                                {
                                    scanInfo.Charge = value;
                                }
                            }
                            break;
                    }
                }
                else if (reader.NodeType == XmlNodeType.EndElement && reader.Name == "spectrum")
                {
                    // Store this spectrum
                    if (!string.IsNullOrEmpty(spectrumID))
                    {
                        spectrumIDToScanNumber.Add(spectrumID, scanInfo);
                    }
                }
            }

            return true;
        }

        private string XMLTextReaderGetAttributeValue(XmlReader reader, string attributeName, string valueIfMissing)
        {
            reader.MoveToAttribute(attributeName);

            if (reader.ReadAttributeValue())
            {
                return reader.Value;
            }

            return valueIfMissing;
        }

        [Obsolete("Unused")]
        private string XMLTextReaderGetInnerText(XmlReader reader)
        {
            var value = string.Empty;
            bool success;

            if (reader.NodeType == XmlNodeType.Element)
            {
                // Advance the reader so that we can read the value
                success = reader.Read();
            }
            else
            {
                success = true;
            }

            if (success && reader.NodeType != XmlNodeType.Whitespace && reader.HasValue)
            {
                value = reader.Value;
            }

            return value;
        }

        private void XMLTextReaderSkipWhitespace(XmlReader reader)
        {
            if (reader.NodeType == XmlNodeType.Whitespace)
            {
                // Whitespace; read the next node
                reader.Read();
            }
        }

        private bool UpdateMGFFileTitleLinesUsingMzML(string mzMLFilePath, string mgfFilePath, string datasetName)
        {
            var spectrumIDtoScanNumber = new Dictionary<string, ScanInfo>();

            try
            {
                // Open the mzXML file and look for "spectrum" elements with an "id" attribute
                // Also look for the charge state

                if (mDebugLevel >= 1)
                {
                    OnDebugEvent(
                        "Parsing the .mzML file to create the spectrum ID to scan number mapping");
                }

                var success = ParseMzMLFile(mzMLFilePath, out var autoNumberScans, spectrumIDtoScanNumber);

                if (!success)
                {
                    OnErrorEvent("ParseMzMLFile returned false; aborting");
                    return false;
                }

                if (!autoNumberScans)
                {
                    // Nothing to update; exit this method
                    OnStatusEvent(
                        "Spectrum IDs in the mzML file were in the format StartScan.EndScan.Charge; no need to update the MGF file");
                    return true;
                }

                if (mDebugLevel >= 1)
                {
                    OnDebugEvent("Updating the Title lines in the MGF file");
                }

                var newMGFFilePath = Path.GetTempFileName();

                // Now read the MGF file and update the title lines
                using (var reader = new StreamReader(new FileStream(mgfFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                using (var writer = new StreamWriter(new FileStream(newMGFFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrEmpty(dataLine))
                        {
                            dataLine = string.Empty;
                        }
                        else
                        {
                            if (dataLine.StartsWith("TITLE="))
                            {
                                var title = dataLine.Substring("TITLE=".Length);

                                // Look for title in spectrumIDtoScanNumber
                                if (spectrumIDtoScanNumber.TryGetValue(title, out var scanInfo))
                                {
                                    dataLine = "TITLE=" + datasetName + "." + scanInfo.ScanStart.ToString("0000") + "." +
                                                scanInfo.ScanEnd.ToString("0000") + ".";

                                    if (scanInfo.Charge > 0)
                                    {
                                        // Also append charge
                                        dataLine += scanInfo.Charge;
                                    }
                                }
                            }
                        }

                        writer.WriteLine(dataLine);
                    }
                }

                if (mDebugLevel >= 1)
                {
                    OnDebugEvent(
                        "Update complete; replacing the original .MGF file");
                }

                // Delete the original .mgf file and replace it with newMGFFilePath
                AppUtils.GarbageCollectNow();

                AnalysisToolRunnerBase.DeleteFileWithRetries(mgfFilePath, mDebugLevel);

                var newMGFFile = new FileInfo(newMGFFilePath);
                newMGFFile.MoveTo(mgfFilePath);

                return true;
            }
            catch (Exception ex)
            {
                mErrMsg = "Error updating the MGF file title lines using the .mzML file";
                OnErrorEvent(mErrMsg, ex);
                return false;
            }
        }

        private void MGFtoDTA_ErrorEvent(string message)
        {
            OnErrorEvent(message);

            if (string.IsNullOrEmpty(mErrMsg))
            {
                mErrMsg = "MGFtoDTA_Error: " + message;
            }
            else if (mErrMsg.Length < 300)
            {
                mErrMsg += "; MGFtoDTA_Error: " + message;
            }
        }
    }
}
