using AnalysisManagerBase;
using MSDataFileReader;
using PRISM;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Xml;

namespace AnalysisManagerMsXmlGenPlugIn
{
    public class ParentIonUpdater : EventNotifier
    {
        // Ignore Spelling: indexedmzML, cv, dta, mgf, 2016-Sep-10, peptidome

        private const string XML_ELEMENT_INDEXED_MZML = "indexedmzML";
        private const string XML_ELEMENT_MZML = "mzML";
        private const string XML_ELEMENT_SOFTWARE_LIST = "softwareList";
        private const string XML_ELEMENT_DATA_PROCESSING_LIST = "dataProcessingList";
        private const string XML_ELEMENT_SPECTRUM_LIST = "spectrumList";
        private const string XML_ELEMENT_SPECTRUM = "spectrum";
        private const string XML_ELEMENT_SCAN = "scan";
        private const string XML_ELEMENT_SELECTED_ION = "selectedIon";
        private const string XML_ELEMENT_CV_PARAM = "cvParam";
        private const string XML_ELEMENT_USER_PARAM = "userParam";
        private const string MS_ACCESSION_SELECTED_ION = "MS:1000744";
        private const string MS_ACCESSION_CHARGE_STATE = "MS:1000041";

        private enum cvParamTypeConstants
        {
            Unknown = 0,
            SelectedIonMZ = 1,
            ChargeState = 2
        }

        private struct SoftwareInfo
        {
            public string ID;
            public string Name;
            public string Version;
            public string AccessionCode;
            public string AccessionName;
        }

        private struct ChargeInfo
        {
            public double ParentIonMH;
            public double ParentIonMZ;
            public int Charge;
        }

        /// <summary>
        /// Read a .MGF file and cache the parent ion info for each scan (MH and charge)
        /// </summary>
        /// <param name="mgfFilePath"></param>
        /// <returns>Dictionary where keys are scan number and values are the ParentIon MH and Charge for each scan</returns>
        private Dictionary<int, List<ChargeInfo>> CacheMGFParentIonInfo(string mgfFilePath)
        {
            var cachedParentIonInfo = new Dictionary<int, List<ChargeInfo>>();

            try
            {
                OnProgressUpdate("Caching parent ion info in " + mgfFilePath, 0);

                var mgfFileReader = new MgfFileReader { ReadTextDataOnly = false };

                // Open the MGF file
                mgfFileReader.OpenFile(mgfFilePath);

                // Cache the parent ion m/z and charge values in the MGF file

                while (mgfFileReader.ReadNextSpectrum(out var mgfSpectrum))
                {
                    var currentSpectrum = mgfFileReader.CurrentSpectrum;
                    var chargeInfo = new ChargeInfo
                    {
                        ParentIonMH = currentSpectrum.ParentIonMH,
                        ParentIonMZ = currentSpectrum.ParentIonMZ,
                        Charge = currentSpectrum.ParentIonCharges[0]
                    };

                    if (!cachedParentIonInfo.TryGetValue(mgfSpectrum.ScanNumber, out var chargeInfoList))
                    {
                        chargeInfoList = new List<ChargeInfo>();
                        cachedParentIonInfo.Add(mgfSpectrum.ScanNumber, chargeInfoList);
                    }
                    chargeInfoList.Add(chargeInfo);
                }

                mgfFileReader.CloseFile();
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception reading the MGF file: " + ex.Message);
            }

            return cachedParentIonInfo;
        }

        /// <summary>
        /// Update the parent ion m/z and charge values in dtaFilePath using parent ion info in mgfFilePath
        /// Optionally replace the original _dta.txt file (dtaFilePath) with the updated version
        /// </summary>
        /// <remarks>
        /// Could be used to update a _dta.txt file created using DeconMSn with the parent ion information
        /// from a .mgf file created by RawConverter.
        /// </remarks>
        /// <param name="dtaFilePath">_dta.txt file to update</param>
        /// <param name="mgfFilePath">.mgf file with parent ion m/z and charge values to use when updating the _dta.txt file (e.g. from RawConverter)</param>
        /// <param name="removeUnmatchedSpectra">When true, remove spectra from _dta.txt that are not in the .mgf file</param>
        /// <param name="replaceDtaFile">When true, replace the original _dta.txt file with the updated one</param>
        /// <returns>Path to the updated mzML file</returns>
        [Obsolete("Unused")]
        private string UpdateDtaParentIonInfoUsingMGF(string dtaFilePath, string mgfFilePath, bool removeUnmatchedSpectra, bool replaceDtaFile)
        {
            // Look for the charge state in the title line, for example 2.dta in "DatasetName.5396.5396.2.dta"
            var reDtaChargeUpdater = new Regex(@"\d+\.dta", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            try
            {
                var cachedParentIonInfo = CacheMGFParentIonInfo(mgfFilePath);

                if (cachedParentIonInfo.Count == 0)
                {
                    OnErrorEvent("Error, no data found in MGF file " + mgfFilePath);
                    return string.Empty;
                }

                var sourceDtaFile = new FileInfo(dtaFilePath);
                var updatedDtaFile = new FileInfo(sourceDtaFile.FullName + ".new");

                var dtaFileReader = new DtaTextFileReader(false) { ReadTextDataOnly = true };

                // Open the _DTA.txt file
                dtaFileReader.OpenFile(dtaFilePath);

                using (var writer = new StreamWriter(new FileStream(updatedDtaFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    var lastScan = 0;

                    while (dtaFileReader.ReadNextSpectrum(out var dtaSpectrum))
                    {
                        if (!cachedParentIonInfo.TryGetValue(dtaSpectrum.ScanNumber, out var chargeInfoList))
                        {
                            OnWarningEvent("Warning, scan " + dtaSpectrum.ScanNumber + " not found in MGF file; unable to update the data");

                            if (removeUnmatchedSpectra)
                            {
                                Console.WriteLine("Skipping scan " + dtaSpectrum.ScanNumber);
                            }
                            else
                            {
                                writer.WriteLine(dtaFileReader.GetMostRecentSpectrumFileText());
                            }

                            continue;
                        }

                        if (lastScan == dtaSpectrum.ScanNumber)
                        {
                            // Skip this entry from the source _dta.txt file since it is a duplicate of the previously written scan
                            continue;
                        }

                        // Update the cached scan info
                        lastScan = dtaSpectrum.ScanNumber;

                        var chargeStateOld = dtaFileReader.CurrentSpectrum.ParentIonCharges[0];

                        foreach (var chargeInfo in chargeInfoList)
                        {
                            // Write a blank line before each title line
                            writer.WriteLine();

                            // Write the spectrum title line, for example:
                            // =================================== "CPTAC_Peptidome_Test1_P1_R2_13Jan12_Polaroid_11-10-14.2.2.1.dta" ==================================

                            var titleLine = dtaFileReader.GetSpectrumTitleWithCommentChars();
                            var chargeStateNew = chargeInfo.Charge;

                            if (chargeStateOld != chargeStateNew)
                            {
                                // Charge state has changed; need to update the titleLine
                                titleLine = reDtaChargeUpdater.Replace(titleLine, chargeStateNew + ".dta");
                            }
                            writer.WriteLine(titleLine);

                            // Construct the parent ion line, which for CDTA files is in the format:
                            // MH Charge   scan=Scan cs=Charge

                            // Example:
                            // 445.119822167 1   scan=2 cs=1

                            var scanNumber = dtaFileReader.CurrentSpectrum.ScanNumber;
                            var outLine = chargeInfo.ParentIonMH.ToString("0.0######") + " " + chargeStateNew + "   scan=" + scanNumber + " cs=" +
                                          chargeStateNew;

                            writer.WriteLine(outLine);

                            // Write the ions using the data from the DTA file
                            var dtaMsMsData = dtaFileReader.GetMSMSDataAsText();

                            foreach (var item in dtaMsMsData)
                            {
                                writer.WriteLine(item);
                            }
                        }
                    }
                }

                dtaFileReader.CloseFile();

                if (replaceDtaFile)
                {
                    var oldDtaFilePath = Path.Combine(sourceDtaFile.FullName + ".old");
                    var oldDtaFile = new FileInfo(oldDtaFilePath);

                    if (oldDtaFile.Exists)
                    {
                        oldDtaFile.Delete();
                    }

                    Global.IdleLoop(0.25);
                    sourceDtaFile.MoveTo(oldDtaFilePath);
                    updatedDtaFile.MoveTo(dtaFilePath);

                    return dtaFilePath;
                }

                return updatedDtaFile.FullName;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in UpdateDtaParentIonInfoUsingMGF: " + ex.Message);
                return string.Empty;
            }
        }

        /// <summary>
        /// Update the count attribute in a dataProcessingList or softwareList element
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="writer"></param>
        private void UpdateListCount(XmlReader reader, XmlWriter writer)
        {
            if (reader.HasAttributes)
            {
                // Read and update the count attribute
                while (reader.MoveToNextAttribute())
                {
                    writer.WriteStartAttribute(reader.Name);
                    var valueToWrite = reader.Value;

                    if (reader.Name == "count")
                    {
                        if (int.TryParse(reader.Value, out var listItemCount))
                        {
                            listItemCount++;
                        }
                        else
                        {
                            listItemCount = 1;
                        }
                        valueToWrite = listItemCount.ToString();
                    }

                    writer.WriteString(valueToWrite);
                    writer.WriteEndAttribute();
                }
            }
        }

        /// <summary>
        /// Update the parent ion m/z and charge values in a mzML file using the parent ion info in a .MGF file
        /// </summary>
        /// <param name="mzMLFilePath"></param>
        /// <param name="mgfFilePath"></param>
        /// <param name="replaceMzMLFile"></param>
        /// <returns>Path to the updated mzML file</returns>
        public string UpdateMzMLParentIonInfoUsingMGF(string mzMLFilePath, string mgfFilePath, bool replaceMzMLFile)
        {
            var reScanNumber = new Regex(@"scan=(?<ScanNumber>\d+)", RegexOptions.Compiled);

            try
            {
                var softwareInfo = new SoftwareInfo
                {
                    ID = "RawConverter",
                    // Used to relate a <software> entry with a <dataProcessing> entry
                    Name = "RawConverter",
                    Version = "2016-Sep-10",
                    AccessionCode = string.Empty,
                    AccessionName = string.Empty
                };

                var cachedParentIonInfo = CacheMGFParentIonInfo(mgfFilePath);

                if (cachedParentIonInfo.Count == 0)
                {
                    OnErrorEvent("Error, no data found in MGF file " + mgfFilePath);
                    return string.Empty;
                }

                var sourceMzMLFile = new FileInfo(mzMLFilePath);

                if (sourceMzMLFile.Directory == null)
                {
                    throw new DirectoryNotFoundException("Unable to determine the parent directory of " + mzMLFilePath);
                }

                var newFileName = Path.GetFileNameWithoutExtension(sourceMzMLFile.Name) + "_new" + Path.GetExtension(sourceMzMLFile.Name);
                var updatedMzMLFile = new FileInfo(Path.Combine(sourceMzMLFile.Directory.FullName, newFileName));

                var atEndOfMzML = false;
                var currentScanNumber = -1;
                var updatedScan = false;
                var updatedSelectedIon = false;

                var totalSpectrumCount = 0;
                var spectraRead = 0;
                var lastProgress = DateTime.UtcNow;

                OnProgressUpdate("Processing mzML file " + mzMLFilePath, 0);

                using (var reader = new XmlTextReader(new FileStream(mzMLFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    reader.WhitespaceHandling = WhitespaceHandling.Significant;

                    using var writer = new XmlTextWriter(new FileStream(updatedMzMLFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read),
                        System.Text.Encoding.GetEncoding("UTF-8"))
                    {
                        Formatting = Formatting.Indented,
                        Indentation = 2,
                        IndentChar = ' '
                    };

                    while (reader.Read())
                    {
                        var nodeAlreadyWritten = false;

                        // ReSharper disable once ConvertIfStatementToSwitchStatement
                        if (reader.NodeType == XmlNodeType.Element)
                        {
                            switch (reader.Name)
                            {
                                case XML_ELEMENT_INDEXED_MZML:
                                    // Skip this element since the new file will not have an index
                                    nodeAlreadyWritten = true;

                                    break;
                                case XML_ELEMENT_SOFTWARE_LIST:
                                    WriteUpdatedSoftwareList(reader, writer, softwareInfo);
                                    nodeAlreadyWritten = true;

                                    break;
                                case XML_ELEMENT_DATA_PROCESSING_LIST:
                                    WriteUpdatedDataProcessingList(reader, writer, softwareInfo);
                                    nodeAlreadyWritten = true;

                                    break;
                                case XML_ELEMENT_SPECTRUM_LIST:
                                    if (reader.HasAttributes)
                                    {
                                        writer.WriteStartElement(reader.Prefix, reader.LocalName, reader.NamespaceURI);
                                        nodeAlreadyWritten = true;

                                        while (reader.MoveToNextAttribute())
                                        {
                                            if (reader.Name == "count")
                                            {
                                                int.TryParse(reader.Value, out totalSpectrumCount);
                                                break;
                                            }
                                        }
                                        reader.MoveToFirstAttribute();
                                        writer.WriteAttributes(reader, true);
                                    }

                                    break;
                                case XML_ELEMENT_SPECTRUM:
                                    currentScanNumber = -1;
                                    updatedScan = false;
                                    updatedSelectedIon = false;

                                    if (reader.HasAttributes)
                                    {
                                        writer.WriteStartElement(reader.Prefix, reader.LocalName, reader.NamespaceURI);
                                        nodeAlreadyWritten = true;

                                        while (reader.MoveToNextAttribute())
                                        {
                                            if (reader.Name == "id")
                                            {
                                                // Value should be of the form:
                                                // controllerType=0 controllerNumber=1 scan=2

                                                var scanId = reader.Value;
                                                var reMatch = reScanNumber.Match(scanId);

                                                if (reMatch.Success)
                                                {
                                                    currentScanNumber = int.Parse(reMatch.Groups["ScanNumber"].Value);
                                                }

                                                break;
                                            }
                                        }

                                        reader.MoveToFirstAttribute();
                                        writer.WriteAttributes(reader, true);
                                    }

                                    spectraRead++;

                                    if (DateTime.UtcNow.Subtract(lastProgress).TotalSeconds >= 1)
                                    {
                                        lastProgress = DateTime.UtcNow;

                                        var percentComplete = 0;

                                        if (totalSpectrumCount > 0)
                                        {
                                            percentComplete = (int)(spectraRead / (float)totalSpectrumCount * 100);
                                        }

                                        OnProgressUpdate(string.Format("{0} spectra read; {1}% complete", spectraRead, percentComplete),
                                            percentComplete);
                                    }

                                    break;
                                case XML_ELEMENT_SCAN:
                                    // Assure that we only update the first <scan> (in case there are multiple scans)
                                    if (!updatedScan)
                                    {
                                        updatedScan = true;

                                        if (cachedParentIonInfo.TryGetValue(currentScanNumber, out var chargeInfoList))
                                        {
                                            WriteUpdatedScan(reader, writer, chargeInfoList);
                                            nodeAlreadyWritten = true;
                                        }
                                    }

                                    break;
                                case XML_ELEMENT_SELECTED_ION:
                                    // Assure that we only update the first <selectedIon> (in case there are multiple precursor ions)
                                    if (!updatedSelectedIon)
                                    {
                                        updatedSelectedIon = true;

                                        if (cachedParentIonInfo.TryGetValue(currentScanNumber, out var chargeInfoList))
                                        {
                                            WriteUpdatedSelectedIon(reader, writer, chargeInfoList);
                                            nodeAlreadyWritten = true;
                                        }
                                    }
                                    break;
                            }
                        }
                        else if (reader.NodeType == XmlNodeType.EndElement)
                        {
                            if (reader.Name == XML_ELEMENT_MZML)
                            {
                                // Write this element, then exit
                                atEndOfMzML = true;
                            }
                        }

                        if (!nodeAlreadyWritten)
                        {
                            WriteShallowNode(reader, writer);
                        }

                        if (atEndOfMzML)
                        {
                            break;
                        }
                    }

                    writer.WriteWhitespace("\r");
                }

                if (replaceMzMLFile)
                {
                    var oldMzMLFilePath = Path.Combine(sourceMzMLFile.FullName + ".old");
                    var oldMzMLFile = new FileInfo(oldMzMLFilePath);

                    if (oldMzMLFile.Exists)
                    {
                        oldMzMLFile.Delete();
                    }

                    Global.IdleLoop(0.25);
                    sourceMzMLFile.MoveTo(oldMzMLFilePath);
                    updatedMzMLFile.MoveTo(mzMLFilePath);

                    return mzMLFilePath;
                }

                return updatedMzMLFile.FullName;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in UpdateMzMLParentIonInfoUsingMGF: " + ex.Message);
                return string.Empty;
            }
        }

        private void WriteShallowNode(XmlReader reader, XmlWriter writer)
        {
            switch (reader.NodeType)
            {
                case XmlNodeType.Element:
                    writer.WriteStartElement(reader.Prefix, reader.LocalName, reader.NamespaceURI);
                    writer.WriteAttributes(reader, true);

                    if (reader.IsEmptyElement)
                    {
                        writer.WriteEndElement();
                    }
                    break;

                case XmlNodeType.Text:
                    writer.WriteString(reader.Value);
                    break;

                case XmlNodeType.Whitespace:
                case XmlNodeType.SignificantWhitespace:
                    writer.WriteWhitespace(reader.Value);
                    break;

                case XmlNodeType.CDATA:
                    writer.WriteCData(reader.Value);
                    break;

                case XmlNodeType.EntityReference:
                    writer.WriteEntityRef(reader.Name);
                    break;

                case XmlNodeType.XmlDeclaration:
                    writer.WriteProcessingInstruction(reader.Name, reader.Value);
                    break;

                case XmlNodeType.ProcessingInstruction:
                    writer.WriteProcessingInstruction(reader.Name, reader.Value);
                    break;

                case XmlNodeType.DocumentType:
                    var sysId = reader.GetAttribute("SYSTEM") ?? string.Empty;
                    writer.WriteDocType(reader.Name, reader.GetAttribute("PUBLIC"), sysId, reader.Value);
                    break;

                case XmlNodeType.Comment:
                    writer.WriteComment(reader.Value);
                    break;

                case XmlNodeType.EndElement:
                    writer.WriteFullEndElement();
                    break;
            }
        }

        private void WriteUpdatedScan(XmlReader reader, XmlWriter writer, IReadOnlyCollection<ChargeInfo> chargeInfoList)
        {
            // Write the start element and any attributes
            WriteShallowNode(reader, writer);

            if (reader.IsEmptyElement)
            {
                // No cvParams are present; nothing to update
                return;
            }

            // Read/write each of the CVParams associated with this Scan

            var startDepth = reader.Depth;

            while (reader.Read())
            {
                if (reader.NodeType == XmlNodeType.EndElement)
                {
                    WriteShallowNode(reader, writer);

                    if (reader.Depth == startDepth)
                    {
                        return;
                    }
                }
                else if (reader.NodeType == XmlNodeType.Element && reader.Name == XML_ELEMENT_USER_PARAM && reader.Depth == startDepth + 1 &&
                         reader.HasAttributes)
                {
                    // Possibly update this userParam

                    writer.WriteStartElement(reader.Prefix, reader.LocalName, reader.NamespaceURI);
                    var isThermoTrailerExtra = false;

                    while (reader.MoveToNextAttribute())
                    {
                        writer.WriteStartAttribute(reader.Name);
                        var valueToWrite = reader.Value;

                        if (reader.Value.StartsWith("[Thermo Trailer Extra]Monoisotopic M/Z"))
                        {
                            isThermoTrailerExtra = true;
                        }
                        else if (reader.Name == "value" && isThermoTrailerExtra)
                        {
                            valueToWrite = chargeInfoList.First().ParentIonMZ.ToString("0.0########");
                        }

                        writer.WriteString(valueToWrite);
                        writer.WriteEndAttribute();
                    }

                    writer.WriteEndElement();
                }
                else
                {
                    WriteShallowNode(reader, writer);
                }
            }
        }

        private void WriteUpdatedSelectedIon(XmlReader reader, XmlWriter writer, IReadOnlyCollection<ChargeInfo> chargeInfoList)
        {
            // Write the start element and any attributes
            WriteShallowNode(reader, writer);

            if (reader.IsEmptyElement)
            {
                // No cvParams are present; nothing to update
                return;
            }

            // Read/write each of the CVParams associated with this Selected Ion

            var startDepth = reader.Depth;

            while (reader.Read())
            {
                if (reader.NodeType == XmlNodeType.EndElement)
                {
                    WriteShallowNode(reader, writer);

                    if (reader.Depth == startDepth)
                    {
                        return;
                    }
                }
                else if (reader.NodeType == XmlNodeType.Element && reader.Name == XML_ELEMENT_CV_PARAM && reader.Depth == startDepth + 1 &&
                         reader.HasAttributes)
                {
                    // Possibly update this cvParam

                    writer.WriteStartElement(reader.Prefix, reader.LocalName, reader.NamespaceURI);
                    var cvParamType = cvParamTypeConstants.Unknown;

                    while (reader.MoveToNextAttribute())
                    {
                        writer.WriteStartAttribute(reader.Name);
                        var valueToWrite = reader.Value;

                        if (reader.Name == "accession")
                        {
                            switch (reader.Value)
                            {
                                case MS_ACCESSION_SELECTED_ION:
                                    cvParamType = cvParamTypeConstants.SelectedIonMZ;
                                    break;
                                case MS_ACCESSION_CHARGE_STATE:
                                    cvParamType = cvParamTypeConstants.ChargeState;
                                    break;
                            }
                        }
                        else if (reader.Name == "value")
                        {
                            switch (cvParamType)
                            {
                                case cvParamTypeConstants.SelectedIonMZ:
                                    valueToWrite = chargeInfoList.First().ParentIonMZ.ToString("0.0########");
                                    break;
                                case cvParamTypeConstants.ChargeState:
                                    valueToWrite = chargeInfoList.First().Charge.ToString();
                                    break;
                            }
                        }

                        writer.WriteString(valueToWrite);
                        writer.WriteEndAttribute();
                    }

                    writer.WriteEndElement();
                }
                else
                {
                    WriteShallowNode(reader, writer);
                }
            }
        }

        private void WriteUpdatedDataProcessingList(XmlReader reader, XmlWriter writer, SoftwareInfo softwareInfo)
        {
            if (reader.IsEmptyElement)
            {
                WriteShallowNode(reader, writer);
                return;
            }

            var startDepth = reader.Depth;

            // Write the start element: <dataProcessingList
            writer.WriteStartElement(reader.Prefix, reader.LocalName, reader.NamespaceURI);

            // Increment the count value in <dataProcessingList count="2">
            UpdateListCount(reader, writer);

            // Read/write the dataProcessing list

            while (reader.Read())
            {
                if (reader.NodeType == XmlNodeType.EndElement)
                {
                    if (reader.Depth == startDepth)
                    {
                        // Add a new dataProcessing entry

                        writer.WriteStartElement("dataProcessing");
                        WriteXmlAttribute(writer, "id", softwareInfo.ID + "_Precursor_recalculation");

                        writer.WriteStartElement("processingMethod");
                        WriteXmlAttribute(writer, "order", "0");
                        WriteXmlAttribute(writer, "softwareRef", softwareInfo.ID);

                        writer.WriteStartElement(XML_ELEMENT_CV_PARAM);
                        WriteXmlAttribute(writer, "cvRef", "MS");
                        WriteXmlAttribute(writer, "accession", "MS:1000780");
                        WriteXmlAttribute(writer, "name", "precursor recalculation");
                        WriteXmlAttribute(writer, "value", string.Empty);

                        // Close out the cvParam
                        writer.WriteEndElement();

                        // Write </processingMethod>
                        writer.WriteFullEndElement();

                        // Write </dataProcessing>
                        writer.WriteFullEndElement();

                        // Write </dataProcessingList>
                        writer.WriteFullEndElement();

                        return;
                    }
                }

                WriteShallowNode(reader, writer);
            }
        }

        private void WriteUpdatedSoftwareList(XmlReader reader, XmlWriter writer, SoftwareInfo softwareInfo)
        {
            if (reader.IsEmptyElement)
            {
                WriteShallowNode(reader, writer);
                return;
            }

            var startDepth = reader.Depth;

            // Write the start element: <softwareList
            writer.WriteStartElement(reader.Prefix, reader.LocalName, reader.NamespaceURI);

            // Increment the count value in <softwareList count="2">
            UpdateListCount(reader, writer);

            // Read/write the software list

            while (reader.Read())
            {
                if (reader.NodeType == XmlNodeType.EndElement)
                {
                    if (reader.Depth == startDepth)
                    {
                        // Add a new software entry

                        writer.WriteStartElement("software");
                        WriteXmlAttribute(writer, "id", softwareInfo.ID);
                        WriteXmlAttribute(writer, "version", softwareInfo.Version);

                        writer.WriteStartElement(XML_ELEMENT_CV_PARAM);
                        WriteXmlAttribute(writer, "cvRef", "MS");

                        if (string.IsNullOrEmpty(softwareInfo.AccessionCode))
                        {
                            WriteXmlAttribute(writer, "accession", "MS:1000799");
                            WriteXmlAttribute(writer, "name", "custom unreleased software tool");
                            WriteXmlAttribute(writer, "value", softwareInfo.Name);
                        }
                        else
                        {
                            WriteXmlAttribute(writer, "accession", softwareInfo.AccessionCode);
                            WriteXmlAttribute(writer, "name", softwareInfo.AccessionName);
                            WriteXmlAttribute(writer, "value", softwareInfo.Name);
                        }

                        // Close out the cvParam
                        writer.WriteEndElement();

                        // Write </software>
                        writer.WriteFullEndElement();

                        // Write </softwareList>
                        writer.WriteFullEndElement();

                        return;
                    }
                }

                WriteShallowNode(reader, writer);
            }
        }

        private void WriteXmlAttribute(XmlWriter writer, string name, string value)
        {
            writer.WriteStartAttribute(name);

            if (string.IsNullOrEmpty(value))
            {
                writer.WriteString(string.Empty);
            }
            else
            {
                writer.WriteString(value);
            }

            writer.WriteEndAttribute();
        }
    }
}
