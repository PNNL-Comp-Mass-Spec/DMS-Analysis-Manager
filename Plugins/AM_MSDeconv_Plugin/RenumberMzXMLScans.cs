using MSDataFileReader;
using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;

namespace AnalysisManagerMSDeconvPlugIn
{
    public class RenumberMzXMLScans
    {
        private const string XML_ELEMENT_SCAN = "scan";
        private const string XML_ELEMENT_PRECURSOR_MZ = "precursorMz";

        private Dictionary<int, int> mScanNumMap;
        private int mNextScanNumber;

        public string ErrorMessage { get; private set; } = string.Empty;

        public string SourceMzXmlFile { get; }

        public RenumberMzXMLScans(string sourceFilePath)
        {
            SourceMzXmlFile = sourceFilePath;
        }

        public bool Process(string targetFilePath)
        {
            try
            {
                mScanNumMap = new Dictionary<int, int>();
                mNextScanNumber = 1;
                ErrorMessage = string.Empty;

                using (var reader = new XmlTextReader(new FileStream(SourceMzXmlFile, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    reader.WhitespaceHandling = WhitespaceHandling.Significant;

                    using var writer = new XmlTextWriter(new FileStream(targetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read), Encoding.GetEncoding("ISO-8859-1"))
                    {
                        Formatting = Formatting.Indented,
                        Indentation = 2,
                        IndentChar = ' '
                    };

                    while (reader.Read())
                    {
                        if (reader.NodeType == XmlNodeType.Element)
                        {
                            if (reader.Name == XML_ELEMENT_SCAN)
                            {
                                WriteUpdatedScan(reader, writer);
                            }
                            else if (reader.Name == XML_ELEMENT_PRECURSOR_MZ)
                            {
                                WriteUpdatedPrecursorMz(reader, writer);
                            }
                            else
                            {
                                WriteShallowNode(reader, writer);
                            }
                        }
                        else
                        {
                            WriteShallowNode(reader, writer);
                        }
                    }

                    writer.WriteWhitespace(Environment.NewLine);
                }

                // Regenerate the byte-offset index at the end of the file
                var success = IndexMzXmlFile(targetFilePath);

                return success;
            }
            catch (Exception ex)
            {
                ErrorMessage = "Error in RenumberMzXMLScans.Process: " + ex.Message;
                Console.WriteLine(ErrorMessage);
                return false;
            }
        }

        private bool IndexMzXmlFile(string filePath)
        {
            try
            {
                var scanOffsetMap = new SortedList<int, long>();

                var reScanNum = new Regex(@"<scan.+num=""(\d+)""", RegexOptions.Compiled);

                var originalFile = new FileInfo(filePath);
                var indexedFile = new FileInfo(originalFile.FullName + ".indexed");

                var reader = new clsBinaryTextReader();
                if (!reader.OpenFile(originalFile.FullName))
                    return false;

                var lineTerminator = Environment.NewLine;

                using (var indexedFileStream= new FileStream(indexedFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read))
                using (var writer = new StreamWriter(indexedFileStream))
                {
                    while (reader.ReadLine())
                    {
                        // Look for a match to <scan num="1234"
                        var reMatch = reScanNum.Match(reader.CurrentLine);

                        if (reMatch.Success)
                        {
                            if (int.TryParse(reMatch.Groups[1].ToString(), out var scanNumber))
                            {
                                var currentOffset = reader.CurrentLineByteOffsetStart + reMatch.Index;

                                scanOffsetMap.Add(scanNumber, currentOffset);
                            }
                        }
                        else if (reader.CurrentLine.TrimStart().StartsWith("<index name=\"scan\">"))
                        {
                            // Write out the scan index
                            lineTerminator = reader.CurrentLineTerminator;

                            // Note: adding 2 to the offset because <index has two spaces in front of it
                            var indexOffset = reader.CurrentLineByteOffsetStart + 2;

                            writer.Write("  <index name=\"scan\">" + lineTerminator);

                            foreach (var scanEntry in scanOffsetMap)
                            {
                                writer.Write("    <offset id=\"" + scanEntry.Key + "\">" + scanEntry.Value + "</offset>" + lineTerminator);
                            }

                            writer.Write("  </index>" + lineTerminator);
                            writer.Write("  <indexOffset>" + indexOffset + "</indexOffset>" + lineTerminator);

                            writer.Write("  <sha1>");

                            break;
                        }

                        writer.Write(reader.CurrentLine + reader.CurrentLineTerminator);
                    }

                    writer.Flush();
                }

                reader.Close();

                PRISM.ProgRunner.GarbageCollectNow();

                // Compute the Sha1 hash of the content written from the start, up to and including "  <sha1"
                byte[] hashValue;

                using (var mySha1 = SHA1.Create())
                {
                    using var indexedFileStream = new FileStream(indexedFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read);

                    hashValue = mySha1.ComputeHash(indexedFileStream);
                }

                PRISM.ProgRunner.GarbageCollectNow();

                using (var indexedFileStream = new FileStream(indexedFile.FullName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
                {
                    using var writer = new StreamWriter(indexedFileStream);

                    var sb = new StringBuilder();

                    foreach (var oneByte in hashValue)
                    {
                        sb.AppendFormat("{0:x2}", oneByte);
                    }

                    writer.Write(sb.ToString());

                    writer.Write("</sha1>" + lineTerminator);
                    writer.Write("</mzXML>" + lineTerminator);
                }

                // Replace the target file with the indexed copy
                PRISM.ProgRunner.GarbageCollectNow();

                try
                {
                    if (originalFile.Directory == null)
                        throw new DirectoryNotFoundException("Cannot determine the parent directory of " + originalFile.FullName);

                    var newFilename = Path.GetFileNameWithoutExtension(originalFile.Name) + "_original" + originalFile.Extension;
                    var targetFilePath = Path.Combine(originalFile.Directory.FullName, newFilename);
                    originalFile.MoveTo(targetFilePath);

                    indexedFile.MoveTo(filePath);
                }
                catch (Exception ex)
                {
                    ErrorMessage = "Error replacing the original .mzXML file with the indexed version: " + ex.Message;
                    Console.WriteLine(ErrorMessage);
                    return false;
                }
            }
            catch (Exception ex)
            {
                ErrorMessage = "Error in IndexMzXmlFile: " + ex.Message;
                Console.WriteLine(ErrorMessage);
                return false;
            }

            return true;
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
                    writer.WriteDocType(reader.Name, reader.GetAttribute("PUBLIC"), reader.GetAttribute("SYSTEM"), reader.Value);
                    break;

                case XmlNodeType.Comment:
                    writer.WriteComment(reader.Value);
                    break;

                case XmlNodeType.EndElement:
                    writer.WriteFullEndElement();
                    break;
            }
        }

        private void WriteUpdatedScan(XmlReader reader, XmlWriter writer)
        {
            writer.WriteStartElement(XML_ELEMENT_SCAN);

            while (reader.MoveToNextAttribute())
            {
                writer.WriteStartAttribute(reader.Name);

                if (reader.Name == "num")
                {
                    // Update the scan number
                    var oldScanNumberText = reader.Value;

                    if (int.TryParse(oldScanNumberText, out var oldScanNum))
                    {
                        mScanNumMap.Add(oldScanNum, mNextScanNumber);
                        writer.WriteString(mNextScanNumber.ToString());

                        mNextScanNumber++;
                    }
                    else
                    {
                        throw new Exception("The scan node does not have an integer value for the num attribute: " + oldScanNumberText);
                    }
                }
                else
                {
                    writer.WriteString(reader.Value);
                }

                writer.WriteEndAttribute();
            }
        }

        private void WriteUpdatedPrecursorMz(XmlReader reader, XmlWriter writer)
        {
            writer.WriteStartElement(XML_ELEMENT_PRECURSOR_MZ);

            while (reader.MoveToNextAttribute())
            {
                writer.WriteStartAttribute(reader.Name);

                if (reader.Name == "precursorScanNum")
                {
                    // Update the scan number of the precursor ion
                    var oldScanNumberText = reader.Value;

                    if (int.TryParse(oldScanNumberText, out var oldScanNum))
                    {
                        if (mScanNumMap.TryGetValue(oldScanNum, out var newScanNum))
                        {
                            writer.WriteString(newScanNum.ToString());
                        }
                    }
                    else
                    {
                        throw new Exception("The precursorMz node does not have an integer value for the precursorScanNum attribute: " + oldScanNumberText);
                    }
                }
                else
                {
                    writer.WriteString(reader.Value);
                }

                writer.WriteEndAttribute();
            }
        }
    }
}
