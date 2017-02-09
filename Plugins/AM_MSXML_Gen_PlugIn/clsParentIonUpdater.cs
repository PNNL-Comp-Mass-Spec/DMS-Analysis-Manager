Imports System.Collections.Generic
Imports System.IO
Imports System.Linq
Imports System.Text.RegularExpressions
Imports System.Threading
Imports System.Xml
Imports AnalysisManagerBase

Public Class clsParentIonUpdater
    Inherits clsEventNotifier

    Private Const XML_ELEMENT_INDEXED_MZML As String = "indexedmzML"
    Private Const XML_ELEMENT_MZML As String = "mzML"
    Private Const XML_ELEMENT_SOFTWARE_LIST As String = "softwareList"
    Private Const XML_ELEMENT_DATA_PROCESSING_LIST As String = "dataProcessingList"
    Private Const XML_ELEMENT_SPECTRUM_LIST As String = "spectrumList"
    Private Const XML_ELEMENT_SPECTRUM As String = "spectrum"
    Private Const XML_ELEMENT_SCAN AS String = "scan"
    Private Const XML_ELEMENT_SELECTED_ION As String = "selectedIon"
    Private Const XML_ELEMENT_CV_PARAM As String = "cvParam"
    Private Const XML_ELEMENT_USER_PARAM As String = "userParam"
    Private Const MS_ACCESSION_SELECTED_ION As String = "MS:1000744"
    Private Const MS_ACCESSION_CHARGE_STATE As String = "MS:1000041"

    Private Enum cvParamTypeConstants
        Unknown = 0
        SelectedIonMZ = 1
        ChargeState = 2
    End Enum

    Private Structure udtSoftwareInfoType
        Public ID As String
        Public Name As String
        Public Version As String
        Public AccessionCode As String
        Public AccessionName As String
    End Structure

    Private Structure udtChargeInfoType
        Public ParentIonMH As Double
        Public ParentIonMZ As Double
        Public Charge As Integer
    End Structure

    ''' <summary>
    ''' Read a .MGF file and cache the parent ion info for each scan (MH and charge) 
    ''' </summary>
    ''' <param name="mgfFilePath"></param>
    ''' <returns>Dictionary where keys are scan number and values are the ParentIon MH and Charge for each scan</returns>
    Private Function CacheMGFParentIonInfo(mgfFilePath As String) As Dictionary(Of Integer, List(Of udtChargeInfoType))

        Dim cachedParentIonInfo = New Dictionary(Of Integer, List(Of udtChargeInfoType))

        Try

            OnProgressUpdate("Caching parent ion info in " & mgfFilePath, 0)

            Dim mgfFileReader = New MSDataFileReader.clsMGFFileReader() With {
                .ReadTextDataOnly = False
            }

            ' Open the MGF file
            mgfFileReader.OpenFile(mgfFilePath)

            ' Cache the parent ion m/z and charge values in the MGF file

            Dim objMGFSpectrum As MSDataFileReader.clsSpectrumInfo = Nothing
            Do While mgfFileReader.ReadNextSpectrum(objMGFSpectrum)
                Dim objCurrentSpectrum = mgfFileReader.CurrentSpectrum
                Dim udtChargeInfo = New udtChargeInfoType With {
                    .ParentIonMH = objCurrentSpectrum.ParentIonMH,
                    .ParentIonMZ = objCurrentSpectrum.ParentIonMZ,
                    .Charge = objCurrentSpectrum.ParentIonCharges(0)
                }

                Dim chargeInfoList As List(Of udtChargeInfoType) = Nothing
                If Not cachedParentIonInfo.TryGetValue(objMGFSpectrum.ScanNumber, chargeInfoList) Then
                    chargeInfoList = New List(Of udtChargeInfoType)
                    cachedParentIonInfo.Add(objMGFSpectrum.ScanNumber, chargeInfoList)
                End If
                chargeInfoList.Add(udtChargeInfo)
            Loop

            mgfFileReader.CloseFile()
        Catch ex As Exception
            OnErrorEvent("Exception reading the MGF file: " & ex.Message)
        End Try

        Return cachedParentIonInfo

    End Function

    ''' <summary>
    ''' Update the parent ion m/z and charge values in dtaFilePath using parent ion info in mgfFilePath
    ''' Optionally replace the original _dta.txt file (dtaFilePath) with the updated version
    ''' </summary>
    ''' <param name="dtaFilePath">_dta.txt file to update</param>
    ''' <param name="mgfFilePath">.mgf file with parent ion m/z and charge values to use when updating the _dta.txt file (e.g. from RawConverter)</param>
    ''' <param name="removeUnmatchedSpectra">When true, remove spectra from _dta.txt that are not in the .mgf file</param>
    ''' <param name="replaceDtaFile">When true, replace the original _dta.txt file with the updated one</param>
    ''' <returns>Path to the updated mzML file</returns>
    ''' <remarks>
    ''' Could be used to update a _dta.txt file created using DeconMSn with the parent ion information 
    ''' from a .mgf file created by RawConverter.
    ''' </remarks>
    Private Function UpdateDtaParentIonInfoUsingMGF(dtaFilePath As String, mgfFilePath As String, removeUnmatchedSpectra As Boolean, replaceDtaFile As Boolean) As String

        ' Look for the charge state in the title line, for example 2.dta in "DatasetName.5396.5396.2.dta"
        Dim reDtaChargeUpdater = New Regex("\d+\.dta", RegexOptions.Compiled Or RegexOptions.IgnoreCase)

        Try

            Dim cachedParentIonInfo As Dictionary(Of Integer, List(Of udtChargeInfoType)) = CacheMGFParentIonInfo(mgfFilePath)

            If cachedParentIonInfo.Count = 0 Then
                OnErrorEvent("Error, no data found in MGF file " & mgfFilePath)
                Return String.Empty
            End If

            Dim sourceDtaFile = New FileInfo(dtaFilePath)
            Dim updatedDtaFile = New FileInfo(sourceDtaFile.FullName & ".new")

            Dim dtaFileReader = New MSDataFileReader.clsDtaTextFileReader(False) With {
                .ReadTextDataOnly = True
            }

            ' Open the _DTA.txt file
            dtaFileReader.OpenFile(dtaFilePath)

            Using writer = New StreamWriter(New FileStream(updatedDtaFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read))

                Dim lastScan = 0

                Dim objDTASpectrum As MSDataFileReader.clsSpectrumInfo = Nothing
                While dtaFileReader.ReadNextSpectrum(objDTASpectrum)

                    Dim chargeInfoList As List(Of udtChargeInfoType) = Nothing
                    If Not cachedParentIonInfo.TryGetValue(objDTASpectrum.ScanNumber, chargeInfoList) Then

                        OnWarningEvent("Warning, scan " & objDTASpectrum.ScanNumber & " not found in MGF file; unable to update the data")
                        If removeUnmatchedSpectra Then
                            Console.WriteLine("Skipping scan " & objDTASpectrum.ScanNumber)
                        Else
                            writer.WriteLine(dtaFileReader.GetMostRecentSpectrumFileText())
                        End If

                        Continue While
                    End If

                    If lastScan = objDTASpectrum.ScanNumber Then
                        ' Skip this entry from the source _dta.txt file since it is a duplicate of the previously written scan
                        Continue While
                    End If

                    ' Update the cached scan info
                    lastScan = objDTASpectrum.ScanNumber

                    Dim chargeStateOld = dtaFileReader.CurrentSpectrum.ParentIonCharges(0)

                    For Each udtChargeInfo In chargeInfoList

                        ' Write a blank link before each title line
                        writer.WriteLine()

                        ' Write the spectrum title line, for example:
                        ' =================================== "CPTAC_Peptidome_Test1_P1_R2_Poroshell_03Feb12_Frodo_Poroshell300SB.2.2.1.dta" ==================================

                        Dim titleLine = dtaFileReader.GetSpectrumTitleWithCommentChars()
                        Dim chargeStateNew = udtChargeInfo.Charge

                        If chargeStateOld <> chargeStateNew Then
                            ' Charge state has changed; need to update the titleLine
                            titleLine = reDtaChargeUpdater.Replace(titleLine, chargeStateNew.ToString() & ".dta")
                        End If
                        writer.WriteLine(titleLine)

                        ' Construct the parent ion line, which for CDTA files is in the format:
                        ' MH Charge   scan=Scan cs=Charge
                        '
                        ' Example:
                        ' 445.119822167 1   scan=2 cs=1
                        ' 

                        Dim scanNumber = dtaFileReader.CurrentSpectrum.ScanNumber
                        Dim outLine = udtChargeInfo.ParentIonMH.ToString("0.0######") & " " & chargeStateNew &
                            "   scan=" & scanNumber &
                            " cs=" & chargeStateNew

                        writer.WriteLine(outLine)

                        ' Write the ions using the data from the DTA file
                        Dim dtaMsMsData = dtaFileReader.GetMSMSDataAsText()
                        For Each strItem As String In dtaMsMsData
                            writer.WriteLine(strItem)
                        Next

                    Next

                End While

            End Using

            dtaFileReader.CloseFile()

            If replaceDtaFile Then

                Dim oldDtaFilePath = Path.Combine(sourceDtaFile.FullName & ".old")
                Dim oldDtaFile = New FileInfo(oldDtaFilePath)
                If oldDtaFile.Exists Then
                    oldDtaFile.Delete()
                End If

                Thread.Sleep(250)
                sourceDtaFile.MoveTo(oldDtaFilePath)

                Thread.Sleep(250)
                updatedDtaFile.MoveTo(dtaFilePath)

                Return dtaFilePath
            Else
                Return updatedDtaFile.FullName
            End If

        Catch ex As Exception
            OnErrorEvent("Exception in UpdateDtaParentIonInfoUsingMGF: " & ex.Message)
            Return String.Empty
        End Try

    End Function

    ''' <summary>
    ''' Update the count attribute in a dataProcessingList or softwareList element
    ''' </summary>
    ''' <param name="reader"></param>
    ''' <param name="writer"></param>
    Private Sub UpdateListCount(reader As XmlReader, writer As XmlWriter)
        If reader.HasAttributes Then
            ' Read and update the count attribute
            While reader.MoveToNextAttribute
                writer.WriteStartAttribute(reader.Name)
                Dim valueToWrite = reader.Value

                If reader.Name = "count" Then
                    Dim listItemCount As Integer
                    If Integer.TryParse(reader.Value, listItemCount) Then
                        listItemCount += 1
                    Else
                        listItemCount = 1
                    End If
                    valueToWrite = listItemCount.ToString()
                End If

                writer.WriteString(valueToWrite)
                writer.WriteEndAttribute()
            End While

        End If

    End Sub

    ''' <summary>
    ''' Update the parent ion m/z and charge values in a mzML file using the parent ion info in a .MGF file
    ''' </summary>
    ''' <param name="mzMLFilePath"></param>
    ''' <param name="mgfFilePath"></param>
    ''' <param name="replaceMzMLFile"></param>
    ''' <returns>Path to the updated mzML file</returns>
    Public Function UpdateMzMLParentIonInfoUsingMGF(mzMLFilePath As String, mgfFilePath As String, replaceMzMLFile As Boolean) As String

        Dim reScanNumber = New Regex("scan=(?<ScanNumber>\d+)", RegexOptions.Compiled)

        Try
            Dim softwareInfo As New udtSoftwareInfoType() With {
                .ID = "RawConverter",           ' Used to relate a <software> entry with a <dataProcessing> entry
                .Name = "RawConverter",
                .Version = "2016-Sep-10",
                .AccessionCode = String.Empty,
                .AccessionName = String.Empty
            }

            Dim cachedParentIonInfo As Dictionary(Of Integer, List(Of udtChargeInfoType)) = CacheMGFParentIonInfo(mgfFilePath)

            If cachedParentIonInfo.Count = 0 Then
                OnErrorEvent("Error, no data found in MGF file " & mgfFilePath)
                Return String.Empty
            End If

            Dim sourceMzMLFile = New FileInfo(mzMLFilePath)
            Dim updatedMzMLFile = New FileInfo(Path.Combine(
                                               sourceMzMLFile.Directory.FullName,
                                               Path.GetFileNameWithoutExtension(sourceMzMLFile.Name) & "_new" &
                                               Path.GetExtension(sourceMzMLFile.Name)))

            Dim atEndOfMzML = False
            Dim currentScanNumber As Integer = -1
            Dim updatedScan = False
            Dim updatedSelectedIon = False

            Dim totalSpectrumCount = 0
            Dim spectraRead = 0
            Dim dtLastProgress = DateTime.UtcNow

            OnProgressUpdate("Processing mzML file " & mzMLFilePath, 0)

            Using reader = New XmlTextReader(New FileStream(mzMLFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                reader.WhitespaceHandling = WhitespaceHandling.Significant

                Using writer = New XmlTextWriter(New FileStream(updatedMzMLFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read), Text.Encoding.GetEncoding("UTF-8"))

                    writer.Formatting = Formatting.Indented
                    writer.Indentation = 2
                    writer.IndentChar = " "c

                    While reader.Read()

                        Dim nodeAlreadyWritten = False

                        If reader.NodeType = XmlNodeType.Element Then
                            Select Case reader.Name
                                Case XML_ELEMENT_INDEXED_MZML
                                    ' Skip this element since the new file will not have an index
                                    nodeAlreadyWritten = True

                                Case XML_ELEMENT_SOFTWARE_LIST
                                    WriteUpdatedSoftwareList(reader, writer, softwareInfo)
                                    nodeAlreadyWritten = True

                                Case XML_ELEMENT_DATA_PROCESSING_LIST
                                    WriteUpdatedDataProcessingList(reader, writer, softwareInfo)
                                    nodeAlreadyWritten = True

                                Case XML_ELEMENT_SPECTRUM_LIST
                                    If reader.HasAttributes Then
                                        writer.WriteStartElement(reader.Prefix, reader.LocalName, reader.NamespaceURI)
                                        nodeAlreadyWritten = True
                                        While reader.MoveToNextAttribute
                                            If reader.Name = "count" Then
                                                Integer.TryParse(reader.Value, totalSpectrumCount)
                                                Exit While
                                            End If
                                        End While
                                        reader.MoveToFirstAttribute()
                                        writer.WriteAttributes(reader, True)
                                    End If

                                Case XML_ELEMENT_SPECTRUM
                                    currentScanNumber = -1
                                    updatedScan = False
                                    updatedSelectedIon = False

                                    If reader.HasAttributes Then
                                        writer.WriteStartElement(reader.Prefix, reader.LocalName, reader.NamespaceURI)
                                        nodeAlreadyWritten = True
                                        While reader.MoveToNextAttribute
                                            If reader.Name = "id" Then
                                                ' Value should be of the form:
                                                ' controllerType=0 controllerNumber=1 scan=2

                                                Dim scanId = reader.Value
                                                Dim reMatch = reScanNumber.Match(scanId)

                                                If reMatch.Success Then
                                                    currentScanNumber = CInt(reMatch.Groups("ScanNumber").Value)
                                                End If

                                                Exit While
                                            End If
                                        End While

                                        reader.MoveToFirstAttribute()
                                        writer.WriteAttributes(reader, True)
                                    End If

                                    spectraRead += 1
                                    If DateTime.UtcNow.Subtract(dtLastProgress).TotalSeconds >= 1 Then
                                        dtLastProgress = DateTime.UtcNow

                                        Dim percentComplete = 0
                                        If totalSpectrumCount > 0 Then
                                            percentComplete = CInt(spectraRead / CDbl(totalSpectrumCount) * 100)
                                        End If

                                        OnProgressUpdate(String.Format("{0} spectra read; {1}% complete", spectraRead, percentComplete), percentComplete)
                                    End If

                                Case XML_ELEMENT_SCAN
                                    ' Assure that we only update the first <scan> (in case there are multiple scans)
                                    If Not updatedScan Then
                                        updatedScan = True

                                        Dim chargeInfoList As List(Of udtChargeInfoType) = Nothing
                                        If cachedParentIonInfo.TryGetValue(currentScanNumber, chargeInfoList) Then
                                            WriteUpdatedScan(reader, writer, chargeInfoList)
                                            nodeAlreadyWritten = True
                                        End If
                                    End If

                                Case XML_ELEMENT_SELECTED_ION
                                    ' Assure that we only update the first <selectedIon> (in case there are multiple precursor ions)
                                    If Not updatedSelectedIon Then
                                        updatedSelectedIon = True

                                        Dim chargeInfoList As List(Of udtChargeInfoType) = Nothing
                                        If cachedParentIonInfo.TryGetValue(currentScanNumber, chargeInfoList) Then
                                            WriteUpdatedSelectedIon(reader, writer, chargeInfoList)
                                            nodeAlreadyWritten = True
                                        End If
                                    End If
                            End Select

                        ElseIf reader.NodeType = XmlNodeType.EndElement Then
                            If reader.Name = XML_ELEMENT_MZML Then
                                ' Write this element, then exit
                                atEndOfMzML = True
                            End If
                        End If

                        If Not nodeAlreadyWritten Then
                            WriteShallowNode(reader, writer)
                        End If

                        If atEndOfMzML Then
                            Exit While
                        End If
                    End While

                    writer.WriteWhitespace(ControlChars.NewLine)
                End Using
            End Using

            If replaceMzMLFile Then

                Dim oldMzMLFilePath = Path.Combine(sourceMzMLFile.FullName & ".old")
                Dim oldMzMLFile = New FileInfo(oldMzMLFilePath)
                If oldMzMLFile.Exists Then
                    oldMzMLFile.Delete()
                End If

                Thread.Sleep(250)
                sourceMzMLFile.MoveTo(oldMzMLFilePath)

                Thread.Sleep(250)
                updatedMzMLFile.MoveTo(mzMLFilePath)

                Return mzMLFilePath
            Else
                Return updatedMzMLFile.FullName
            End If

        Catch ex As Exception
            OnErrorEvent("Exception in UpdateMzMLParentIonInfoUsingMGF: " & ex.Message)
            Return String.Empty
        End Try

    End Function

    Private Sub WriteShallowNode(reader As XmlTextReader, writer As XmlTextWriter)

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

    Private Sub WriteUpdatedScan(reader As XmlTextReader, writer As XmlTextWriter, chargeInfoList As List(Of udtChargeInfoType))

        ' Write the start element and any attributes
        WriteShallowNode(reader, writer)

        If reader.IsEmptyElement Then
            ' No cvParams are present; nothing to update
            Return
        End If

        ' Read/write each of the CVParams associated with this Scan

        Dim startDepth = reader.Depth
        While reader.Read()

            If reader.NodeType = XmlNodeType.EndElement Then
                WriteShallowNode(reader, writer)
                If reader.Depth = startDepth Then
                    Return
                End If
            ElseIf reader.NodeType = XmlNodeType.Element AndAlso
               reader.Name = XML_ELEMENT_USER_PARAM AndAlso
               reader.Depth = startDepth + 1 AndAlso
               reader.HasAttributes Then

                ' Possibly update this userParam

                writer.WriteStartElement(reader.Prefix, reader.LocalName, reader.NamespaceURI)
                Dim isThermoTrailerExtra = False

                While reader.MoveToNextAttribute
                    writer.WriteStartAttribute(reader.Name)
                    Dim valueToWrite = reader.Value

                    If reader.Value.StartsWith("[Thermo Trailer Extra]Monoisotopic M/Z") Then
                        isThermoTrailerExtra = True
                    ElseIf reader.Name = "value" AndAlso isThermoTrailerExtra Then
                        valueToWrite = chargeInfoList.First().ParentIonMZ.ToString("0.0########")
                    End If

                    writer.WriteString(valueToWrite)
                    writer.WriteEndAttribute()
                End While

                writer.WriteEndElement()
            Else
                WriteShallowNode(reader, writer)
            End If

        End While

    End Sub

    Private Sub WriteUpdatedSelectedIon(reader As XmlTextReader, writer As XmlTextWriter, chargeInfoList As List(Of udtChargeInfoType))

        ' Write the start element and any attributes
        WriteShallowNode(reader, writer)

        If reader.IsEmptyElement Then
            ' No cvParams are present; nothing to update
            Return
        End If

        ' Read/write each of the CVParams associated with this Selected Ion

        Dim startDepth = reader.Depth
        While reader.Read()

            If reader.NodeType = XmlNodeType.EndElement Then
                WriteShallowNode(reader, writer)
                If reader.Depth = startDepth Then
                    Return
                End If
            ElseIf reader.NodeType = XmlNodeType.Element AndAlso
               reader.Name = XML_ELEMENT_CV_PARAM AndAlso
               reader.Depth = startDepth + 1 AndAlso
               reader.HasAttributes Then

                ' Possibly update this cvParam

                writer.WriteStartElement(reader.Prefix, reader.LocalName, reader.NamespaceURI)
                Dim cvParamType = cvParamTypeConstants.Unknown

                While reader.MoveToNextAttribute
                    writer.WriteStartAttribute(reader.Name)
                    Dim valueToWrite = reader.Value

                    If reader.Name = "accession" Then

                        Select Case reader.Value
                            Case MS_ACCESSION_SELECTED_ION
                                cvParamType = cvParamTypeConstants.SelectedIonMZ
                            Case MS_ACCESSION_CHARGE_STATE
                                cvParamType = cvParamTypeConstants.ChargeState
                        End Select

                    ElseIf reader.Name = "value" Then
                        Select Case cvParamType
                            Case cvParamTypeConstants.SelectedIonMZ
                                valueToWrite = chargeInfoList.First().ParentIonMZ.ToString("0.0########")
                            Case cvParamTypeConstants.ChargeState
                                valueToWrite = chargeInfoList.First().Charge.ToString()
                        End Select
                    End If

                    writer.WriteString(valueToWrite)
                    writer.WriteEndAttribute()
                End While

                writer.WriteEndElement()
            Else
                WriteShallowNode(reader, writer)
            End If

        End While

    End Sub

    Private Sub WriteUpdatedDataProcessingList(reader As XmlTextReader, writer As XmlTextWriter, softwareInfo As udtSoftwareInfoType)

        If reader.IsEmptyElement Then
            WriteShallowNode(reader, writer)
            Return
        End If

        Dim startDepth = reader.Depth

        ' Write the start element: <dataProcessingList
        writer.WriteStartElement(reader.Prefix, reader.LocalName, reader.NamespaceURI)

        ' Increment the count value in <dataProcessingList count="2">
        UpdateListCount(reader, writer)

        ' Read/write the dataProcessing list
        While reader.Read()

            If reader.NodeType = XmlNodeType.EndElement Then

                If reader.Depth = startDepth Then
                    ' Add a new dataProcessing entry

                    writer.WriteStartElement("dataProcessing")
                    WriteXmlAttribute(writer, "id", softwareInfo.ID & "_Precursor_recalculation")

                    writer.WriteStartElement("processingMethod")
                    WriteXmlAttribute(writer, "order", "0")
                    WriteXmlAttribute(writer, "softwareRef", softwareInfo.ID)

                    writer.WriteStartElement(XML_ELEMENT_CV_PARAM)
                    WriteXmlAttribute(writer, "cvRef", "MS")
                    WriteXmlAttribute(writer, "accession", "MS:1000780")
                    WriteXmlAttribute(writer, "name", "precursor recalculation")
                    WriteXmlAttribute(writer, "value", String.Empty)

                    ' Close out the cvParam
                    writer.WriteEndElement()

                    ' Write </processingMethod>
                    writer.WriteFullEndElement()

                    ' Write </dataProcessing>
                    writer.WriteFullEndElement()

                    ' Write </dataProcessingList>
                    writer.WriteFullEndElement()

                    Return
                End If
            End If

            WriteShallowNode(reader, writer)

        End While

    End Sub

    Private Sub WriteUpdatedSoftwareList(reader As XmlTextReader, writer As XmlTextWriter, softwareInfo As udtSoftwareInfoType)

        If reader.IsEmptyElement Then
            WriteShallowNode(reader, writer)
            Return
        End If

        Dim startDepth = reader.Depth

        ' Write the start element: <softwareList
        writer.WriteStartElement(reader.Prefix, reader.LocalName, reader.NamespaceURI)

        ' Increment the count value in <softwareList count="2">
        UpdateListCount(reader, writer)

        ' Read/write the software list
        While reader.Read()

            If reader.NodeType = XmlNodeType.EndElement Then

                If reader.Depth = startDepth Then
                    ' Add a new software entry

                    writer.WriteStartElement("software")
                    WriteXmlAttribute(writer, "id", softwareInfo.ID)
                    WriteXmlAttribute(writer, "version", softwareInfo.Version)

                    writer.WriteStartElement(XML_ELEMENT_CV_PARAM)
                    WriteXmlAttribute(writer, "cvRef", "MS")
                    If String.IsNullOrEmpty(softwareInfo.AccessionCode) Then
                        WriteXmlAttribute(writer, "accession", "MS:1000799")
                        WriteXmlAttribute(writer, "name", "custom unreleased software tool")
                        WriteXmlAttribute(writer, "value", softwareInfo.Name)
                    Else
                        WriteXmlAttribute(writer, "accession", softwareInfo.AccessionCode)
                        WriteXmlAttribute(writer, "name", softwareInfo.AccessionName)
                        WriteXmlAttribute(writer, "value", softwareInfo.Name)
                    End If

                    ' Close out the cvParam
                    writer.WriteEndElement()

                    ' Write </software>
                    writer.WriteFullEndElement()

                    ' Write </softwareList>
                    writer.WriteFullEndElement()

                    Return
                End If
            End If

            WriteShallowNode(reader, writer)

        End While

    End Sub

    Private Sub WriteXmlAttribute(writer As XmlTextWriter, name As String, value As String)

        writer.WriteStartAttribute(name)
        If String.IsNullOrEmpty(value) Then
            writer.WriteString(String.Empty)
        Else
            writer.WriteString(value)
        End If

        writer.WriteEndAttribute()
    End Sub

End Class
