using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Xml;
using MSDataFileReader;

namespace AnalysisManagerMSDeconvPlugIn
{
    public class clsRenumberMzXMLScans
    {
        protected const string XML_ELEMENT_SCAN = "scan";
        protected const string XML_ELEMENT_PRECURSOR_MZ = "precursorMz";

        private string mErrorMessage = string.Empty;
        private string mSourceMzXmlFile;

        private Dictionary<int, int> mScanNumMap;
        private int mNextScanNumber;

        public string ErrorMessage
        {
            get { return mErrorMessage; }
        }

        public string SourceMzXmlFile
        {
            get { return mSourceMzXmlFile; }
            private set { mSourceMzXmlFile = value; }
        }

        public clsRenumberMzXMLScans(string sourceFilePath)
        {
            mSourceMzXmlFile = sourceFilePath;
        }

        public bool Process(string targetFilePath)
        {
            try
            {
                mScanNumMap = new Dictionary<int, int>();
                mNextScanNumber = 1;
                mErrorMessage = string.Empty;

                using (var reader = new XmlTextReader(new FileStream(mSourceMzXmlFile, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    reader.WhitespaceHandling = WhitespaceHandling.Significant;

                    using (var writer = new XmlTextWriter(new FileStream(targetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read), System.Text.Encoding.GetEncoding("ISO-8859-1")))
                    {
                        writer.Formatting = Formatting.Indented;
                        writer.Indentation = 2;
                        writer.IndentChar = ' ';

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
                }

                // Wait 250 msec, then re-generate the byte-offset index at the end of the file
                Thread.Sleep(250);

                var blnSuccess = IndexMzXmlFile(targetFilePath);

                return blnSuccess;
            }
            catch (Exception ex)
            {
                mErrorMessage = "Error in clsRenumberMzXMLScans.Process: " + ex.Message;
                Console.WriteLine(mErrorMessage);
                return false;
            }
        }

        protected bool IndexMzXmlFile(string filePath)
        {
            try
            {
                var scanOffsetMap = new SortedList<int, long>();

                var reScanNum = new Regex(@"<scan.+num=""(\d+)""", RegexOptions.Compiled);

                var fiOriginalFile = new FileInfo(filePath);
                var fiIndexedFile = new FileInfo(fiOriginalFile.FullName + ".indexed");

                var reader = new clsBinaryTextReader();
                if (!reader.OpenFile(fiOriginalFile.FullName))
                    return false;

                var lineTerminator = Environment.NewLine;

                using (var fsIndexedFile = new FileStream(fiIndexedFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read))
                using (var writer = new StreamWriter(fsIndexedFile))
                {
                    while (reader.ReadLine())
                    {
                        // Look for a match to <scan num="1234"
                        var reMatch = reScanNum.Match(reader.CurrentLine);

                        if (reMatch.Success)
                        {
                            var scanNumber = 0;
                            if (int.TryParse(reMatch.Groups[1].ToString(), out scanNumber))
                            {
                                var currentOffset = reader.CurrentLineByteOffsetStart + reMatch.Index;

                                scanOffsetMap.Add(scanNumber, currentOffset);
                            }
                        }
                        else if (reader.CurrentLine.TrimStart().StartsWith("<index name=\"scan\">"))
                        {
                            // Write out the scan index
                            lineTerminator = reader.CurrentLineTerminator;

                            // Note: adding 2 to the offset becauser <index has two spaces in front of it
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

                Thread.Sleep(100);
                PRISM.clsProgRunner.GarbageCollectNow();
                Thread.Sleep(100);

                // Compute the Sha1 hash of the content written from the start, up to and including "  <sha1"
                byte[] hashValue = null;

                using (var mySha1 = SHA1.Create())
                {
                    using (var fsIndexedFile = new FileStream(fiIndexedFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
                    {
                        hashValue = mySha1.ComputeHash(fsIndexedFile);
                    }
                    mySha1.Dispose();
                }

                Thread.Sleep(100);
                PRISM.clsProgRunner.GarbageCollectNow();
                Thread.Sleep(100);

                using (var fsIndexedFile = new FileStream(fiIndexedFile.FullName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
                {
                    using (var writerAppend = new StreamWriter(fsIndexedFile))
                    {
                        var sb = new StringBuilder();

                        foreach (var oneByte in hashValue)
                        {
                            sb.AppendFormat("{0:x2}", oneByte);
                        }

                        writerAppend.Write(sb.ToString());

                        writerAppend.Write("</sha1>" + lineTerminator);
                        writerAppend.Write("</mzXML>" + lineTerminator);
                    }
                }

                // Replace the target file with the indexed copy
                Thread.Sleep(100);
                PRISM.clsProgRunner.GarbageCollectNow();
                Thread.Sleep(100);

                try
                {
                    var targetFilePath = Path.Combine(fiOriginalFile.Directory.FullName,
                        Path.GetFileNameWithoutExtension(fiOriginalFile.Name) + "_original" + fiOriginalFile.Extension);
                    fiOriginalFile.MoveTo(targetFilePath);

                    Thread.Sleep(100);

                    fiIndexedFile.MoveTo(filePath);
                }
                catch (Exception)
                {
                    mErrorMessage = "Error replacing the original .mzXML file with the indexed version";
                    Console.WriteLine(mErrorMessage);
                    return false;
                }
            }
            catch (Exception ex)
            {
                mErrorMessage = "Error in IndexMzXmlFile: " + ex.Message;
                Console.WriteLine(mErrorMessage);
                return false;
            }

            return true;
        }

        protected void WriteShallowNode(XmlTextReader reader, XmlTextWriter writer)
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

        protected void WriteUpdatedScan(XmlTextReader reader, XmlTextWriter writer)
        {
            writer.WriteStartElement(XML_ELEMENT_SCAN);

            while (reader.MoveToNextAttribute())
            {
                writer.WriteStartAttribute(reader.Name);

                if (reader.Name == "num")
                {
                    // Update the scan number
                    var oldScanNumberText = reader.Value;
                    var oldScanNum = 0;

                    if (int.TryParse(oldScanNumberText, out oldScanNum))
                    {
                        mScanNumMap.Add(oldScanNum, mNextScanNumber);
                        writer.WriteString(mNextScanNumber.ToString());

                        mNextScanNumber += 1;
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

        protected void WriteUpdatedPrecursorMz(XmlTextReader reader, XmlTextWriter writer)
        {
            writer.WriteStartElement(XML_ELEMENT_PRECURSOR_MZ);

            while (reader.MoveToNextAttribute())
            {
                writer.WriteStartAttribute(reader.Name);

                if (reader.Name == "precursorScanNum")
                {
                    // Update the scan number of the precursor ion
                    var oldScanNumberText = reader.Value;
                    var oldScanNum = 0;
                    var newScanNum = 0;

                    if (int.TryParse(oldScanNumberText, out oldScanNum))
                    {
                        if (mScanNumMap.TryGetValue(oldScanNum, out newScanNum))
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
