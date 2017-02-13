
Imports MSDataFileReader
Imports System.Security.Cryptography
Imports System.IO
Imports System.Xml
Imports System.Text.RegularExpressions
Imports System.Text

Public Class clsRenumberMzXMLScans

    Protected Const XML_ELEMENT_SCAN As String = "scan"
    Protected Const XML_ELEMENT_PRECURSOR_MZ As String = "precursorMz"

    Private mErrorMessage As String = String.Empty
    Private mSourceMzXmlFile As String

    Private mScanNumMap As Dictionary(Of Integer, Integer)
    Private mNextScanNumber As Integer

    Public ReadOnly Property ErrorMessage As String
        Get
            Return mErrorMessage
        End Get
    End Property

    Public Property SourceMzXmlFile As String

        Get
            Return mSourceMzXmlFile
        End Get
        Private Set(value As String)
            mSourceMzXmlFile = value
        End Set
    End Property

    Public Sub New(sourceFilePath As String)
        mSourceMzXmlFile = sourceFilePath
    End Sub

    Public Function Process(targetFilePath As String) As Boolean

        Try

            mScanNumMap = New Dictionary(Of Integer, Integer)
            mNextScanNumber = 1
            mErrorMessage = String.Empty

            Using reader = New XmlTextReader(New FileStream(mSourceMzXmlFile, FileMode.Open, FileAccess.Read, FileShare.Read))
                reader.WhitespaceHandling = WhitespaceHandling.Significant

                Using writer = New XmlTextWriter(New FileStream(targetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read),
                                                 System.Text.Encoding.GetEncoding("ISO-8859-1"))

                    writer.Formatting = Formatting.Indented
                    writer.Indentation = 2
                    writer.IndentChar = " "c

                    While reader.Read()

                        If reader.NodeType = XmlNodeType.Element Then
                            If reader.Name = XML_ELEMENT_SCAN Then
                                WriteUpdatedScan(reader, writer)

                            ElseIf reader.Name = XML_ELEMENT_PRECURSOR_MZ Then
                                WriteUpdatedPrecursorMz(reader, writer)

                            Else
                                WriteShallowNode(reader, writer)
                            End If

                        Else
                            WriteShallowNode(reader, writer)
                        End If

                    End While

                    writer.WriteWhitespace(ControlChars.NewLine)
                End Using

            End Using

            ' Wait 250 msec, then re-generate the byte-offset index at the end of the file
            Threading.Thread.Sleep(250)

            Dim blnSuccess = IndexMzXmlFile(targetFilePath)

            Return blnSuccess

        Catch ex As Exception
            mErrorMessage = "Error in clsRenumberMzXMLScans.Process: " & ex.Message
            Console.WriteLine(mErrorMessage)
            Return False
        End Try


    End Function

    Protected Function IndexMzXmlFile(filePath As String) As Boolean

        Try

            Dim scanOffsetMap = New SortedList(Of Integer, Int64)

            Dim reScanNum = New Regex("<scan.+num=""(\d+)""", RegexOptions.Compiled)
            Dim reMatch As Match

            Dim fiOriginalFile = New FileInfo(filePath)
            Dim fiIndexedFile = New FileInfo(fiOriginalFile.FullName & ".indexed")

            Dim reader = New clsBinaryTextReader()
            If Not reader.OpenFile(fiOriginalFile.FullName) Then Return False

            Dim lineTerminator As String = ControlChars.NewLine

            Using fsIndexedFile = New FileStream(fiIndexedFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)
                Using writer = New StreamWriter(fsIndexedFile)

                    While reader.ReadLine()
                        ' Look for a match to <scan num="1234"
                        reMatch = reScanNum.Match(reader.CurrentLine)

                        If reMatch.Success Then
                            Dim scanNumber As Integer
                            If Integer.TryParse(reMatch.Groups(1).ToString, scanNumber) Then
                                Dim currentOffset As Int64 = reader.CurrentLineByteOffsetStart + reMatch.Index

                                scanOffsetMap.Add(scanNumber, currentOffset)
                            End If
                        ElseIf reader.CurrentLine.TrimStart().StartsWith("<index name=""scan"">") Then
                            ' Write out the scan index
                            lineTerminator = reader.CurrentLineTerminator

                            ' Note: adding 2 to the offset becauser <index has two spaces in front of it
                            Dim indexOffset As Int64 = reader.CurrentLineByteOffsetStart + 2

                            writer.Write("  <index name=""scan"">" & lineTerminator)

                            For Each scanEntry In scanOffsetMap
                                writer.Write("    <offset id=""" & scanEntry.Key & """>" & scanEntry.Value & "</offset>" & lineTerminator)
                            Next

                            writer.Write("  </index>" & lineTerminator)
                            writer.Write("  <indexOffset>" & indexOffset & "</indexOffset>" & lineTerminator)

                            writer.Write("  <sha1>")

                            Exit While
                        End If

                        writer.Write(reader.CurrentLine & reader.CurrentLineTerminator)

                    End While

                    writer.Flush()
                End Using
            End Using

            reader.Close()

            Threading.Thread.Sleep(100)
            PRISM.Processes.clsProgRunner.GarbageCollectNow()
            Threading.Thread.Sleep(100)

            ' Compute the Sha1 hash of the content written from the start, up to and including "  <sha1"
            Dim hashValue As Byte()

            Using mySha1 = SHA1.Create()
                Using fsIndexedFile = New FileStream(fiIndexedFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)
                    hashValue = mySha1.ComputeHash(fsIndexedFile)
                End Using
                mySha1.Dispose()
            End Using

            Threading.Thread.Sleep(100)
            PRISM.Processes.clsProgRunner.GarbageCollectNow()
            Threading.Thread.Sleep(100)

            Using fsIndexedFile = New FileStream(fiIndexedFile.FullName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)
                Using writerAppend = New StreamWriter(fsIndexedFile)
                    Dim sb = New StringBuilder()

                    For Each oneByte In hashValue
                        sb.AppendFormat("{0:x2}", oneByte)
                    Next

                    writerAppend.Write(sb.ToString())

                    writerAppend.Write("</sha1>" & lineTerminator)
                    writerAppend.Write("</mzXML>" & lineTerminator)
                End Using
            End Using


            ' Replace the target file with the indexed copy
            Threading.Thread.Sleep(100)
            PRISM.Processes.clsProgRunner.GarbageCollectNow()
            Threading.Thread.Sleep(100)

            Try
                Dim targetFilePath = Path.Combine(fiOriginalFile.Directory.FullName, Path.GetFileNameWithoutExtension(fiOriginalFile.Name) & "_original" & fiOriginalFile.Extension)
                fiOriginalFile.MoveTo(targetFilePath)

                Threading.Thread.Sleep(100)

                fiIndexedFile.MoveTo(filePath)

            Catch ex As Exception
                mErrorMessage = "Error replacing the original .mzXML file with the indexed version"
                Console.WriteLine(mErrorMessage)
                Return False
            End Try

        Catch ex As Exception
            mErrorMessage = "Error in IndexMzXmlFile: " & ex.Message
            Console.WriteLine(mErrorMessage)
            Return False
        End Try

        Return True

    End Function

    Protected Sub WriteShallowNode(reader As XmlTextReader, writer As XmlTextWriter)

        Select Case reader.NodeType

            Case XmlNodeType.Element
                writer.WriteStartElement(reader.Prefix, reader.LocalName, reader.NamespaceURI)
                writer.WriteAttributes(reader, True)
                If reader.IsEmptyElement Then
                    writer.WriteEndElement()
                End If

            Case XmlNodeType.Text
                writer.WriteString(reader.Value)

            Case XmlNodeType.Whitespace, XmlNodeType.SignificantWhitespace
                writer.WriteWhitespace(reader.Value)

            Case XmlNodeType.CDATA
                writer.WriteCData(reader.Value)

            Case XmlNodeType.EntityReference
                writer.WriteEntityRef(reader.Name)

            Case XmlNodeType.XmlDeclaration
                writer.WriteProcessingInstruction(reader.Name, reader.Value)

            Case XmlNodeType.ProcessingInstruction
                writer.WriteProcessingInstruction(reader.Name, reader.Value)

            Case XmlNodeType.DocumentType
                writer.WriteDocType(reader.Name, reader.GetAttribute("PUBLIC"), reader.GetAttribute("SYSTEM"), reader.Value)

            Case XmlNodeType.Comment
                writer.WriteComment(reader.Value)

            Case XmlNodeType.EndElement
                writer.WriteFullEndElement()

        End Select
    End Sub

    Protected Sub WriteUpdatedScan(reader As XmlTextReader, writer As XmlTextWriter)

        writer.WriteStartElement(XML_ELEMENT_SCAN)

        While reader.MoveToNextAttribute
            writer.WriteStartAttribute(reader.Name)

            If reader.Name = "num" Then
                ' Update the scan number
                Dim oldScanNumberText = reader.Value
                Dim oldScanNum As Integer

                If Integer.TryParse(oldScanNumberText, oldScanNum) Then
                    mScanNumMap.Add(oldScanNum, mNextScanNumber)
                    writer.WriteString(mNextScanNumber.ToString())

                    mNextScanNumber += 1
                Else
                    Throw New Exception("The scan node does not have an integer value for the num attribute: " & oldScanNumberText)
                End If

            Else
                writer.WriteString(reader.Value)
            End If

            writer.WriteEndAttribute()

        End While
    End Sub

    Protected Sub WriteUpdatedPrecursorMz(reader As XmlTextReader, writer As XmlTextWriter)

        writer.WriteStartElement(XML_ELEMENT_PRECURSOR_MZ)

        While reader.MoveToNextAttribute
            writer.WriteStartAttribute(reader.Name)

            If reader.Name = "precursorScanNum" Then
                ' Update the scan number of the precursor ion
                Dim oldScanNumberText = reader.Value
                Dim oldScanNum As Integer
                Dim newScanNum As Integer

                If Integer.TryParse(oldScanNumberText, oldScanNum) Then
                    If mScanNumMap.TryGetValue(oldScanNum, newScanNum) Then
                        writer.WriteString(newScanNum.ToString())
                    End If
                Else
                    Throw New Exception("The precursorMz node does not have an integer value for the precursorScanNum attribute: " & oldScanNumberText)
                End If

            Else
                writer.WriteString(reader.Value)
            End If

            writer.WriteEndAttribute()

        End While
    End Sub
End Class
