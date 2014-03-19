Option Strict On

Imports AnalysisManagerBase
Imports System.Collections.Generic
Imports System.IO

Public Class clsMGFConverter

#Region "Structures"
	Protected Structure udtScanInfoType
		Public ScanStart As Integer
		Public ScanEnd As Integer
		Public Charge As Integer
	End Structure
#End Region

#Region "Member variables"
	Protected m_DebugLevel As Integer
	Protected m_WorkDir As String
	Protected m_ErrMsg As String

	Protected WithEvents mMGFtoDTA As MascotGenericFileToDTA.clsMGFtoDTA

#End Region

#Region "Properties"
	Public ReadOnly Property ErrorMessage() As String
		Get
			If String.IsNullOrEmpty(m_ErrMsg) Then
				Return String.Empty
			Else
				Return m_ErrMsg
			End If
		End Get
	End Property

	Public ReadOnly Property SpectraCountWritten() As Integer
		Get
			If mMGFtoDTA Is Nothing Then
				Return 0
			Else
				Return mMGFtoDTA.SpectraCountWritten
			End If
		End Get
	End Property
#End Region

	Public Sub New(ByVal intDebugLevel As Integer, ByVal strWorkDir As String)
		m_DebugLevel = intDebugLevel
		m_WorkDir = strWorkDir
		m_ErrMsg = String.Empty
	End Sub

	''' <summary>
	''' Convert .mgf file to _DTA.txt using MascotGenericFileToDTA.dll
	''' This functon is called by MakeDTAFilesThreaded
	''' </summary>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Public Function ConvertMGFtoDTA(ByVal eRawDataType As clsAnalysisResources.eRawDataTypeConstants, ByVal strDatasetName As String) As Boolean

		Dim strMGFFilePath As String
		Dim blnSuccess As Boolean

		m_ErrMsg = String.Empty

		If m_DebugLevel > 0 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Converting .MGF file to _DTA.txt")
		End If

		strMGFFilePath = Path.Combine(m_WorkDir, strDatasetName & clsAnalysisResources.DOT_MGF_EXTENSION)

		If eRawDataType = clsAnalysisResources.eRawDataTypeConstants.mzML Then
			' Read the .mzML file to construct a mapping between "title" line and scan number
			' If necessary, update the .mgf file to have new "title" lines that clsMGFtoDTA will recognize

			Dim strMzMLFilePath As String
			strMzMLFilePath = Path.Combine(m_WorkDir, strDatasetName & clsAnalysisResources.DOT_MZML_EXTENSION)

			blnSuccess = UpdateMGFFileTitleLinesUsingMzML(strMzMLFilePath, strMGFFilePath, strDatasetName)
			If Not blnSuccess Then
				Return False
			End If

		End If

		mMGFtoDTA = New MascotGenericFileToDTA.clsMGFtoDTA()

		With mMGFtoDTA
			.CreateIndividualDTAFiles = False
			.FilterSpectra = False
			.ForceChargeAddnForPredefined2PlusOr3Plus = False
			.GuesstimateChargeForAllSpectra = False
			.LogMessagesToFile = False
			.MaximumIonsPerSpectrum = 0
		End With

		blnSuccess = mMGFtoDTA.ProcessFile(strMGFFilePath, m_WorkDir)

		If Not blnSuccess AndAlso String.IsNullOrEmpty(m_ErrMsg) Then
			m_ErrMsg = mMGFtoDTA.GetErrorMessage()
		End If

		Return blnSuccess

	End Function

	Protected Function GetCVParams(ByRef objXMLReader As Xml.XmlTextReader, ByVal strCurrentElementName As String) As Dictionary(Of String, String)

		Dim lstCVParams As Dictionary(Of String, String) = New Dictionary(Of String, String)()
		Dim strAccession As String
		Dim strValue As String

		Do While objXMLReader.Read()
			XMLTextReaderSkipWhitespace(objXMLReader)

			If objXMLReader.NodeType = Xml.XmlNodeType.EndElement And objXMLReader.Name = strCurrentElementName Then
				Exit Do
			End If

			If objXMLReader.NodeType = Xml.XmlNodeType.Element AndAlso objXMLReader.Name = "cvParam" Then
				strAccession = XMLTextReaderGetAttributeValue(objXMLReader, "accession", String.Empty)
				strValue = XMLTextReaderGetAttributeValue(objXMLReader, "value", String.Empty)

				If Not lstCVParams.ContainsKey(strAccession) Then
					lstCVParams.Add(strAccession, strValue)
				End If
			End If

		Loop
		Return lstCVParams

	End Function

	Protected Function ParseMzMLFile(ByVal strMzMLFilePath As String, ByRef blnAutoNumberScans As Boolean, lstSpectrumIDToScanNumber As Dictionary(Of String, udtScanInfoType)) As Boolean

		Dim strSpectrumID As String = String.Empty

		Dim intScanNumberStart As Integer
		Dim intScanNumberEnd As Integer
		Dim intCharge As Integer
		Dim udtScanInfo As udtScanInfoType

		Dim intScanNumberCurrent As Integer = 0
		Dim strValue As String = String.Empty
		Dim intValue As Integer

		Dim lstCVParams As Dictionary(Of String, String)

		blnAutoNumberScans = False

		If lstSpectrumIDToScanNumber Is Nothing Then
			lstSpectrumIDToScanNumber = New Dictionary(Of String, udtScanInfoType)
		End If

		Using objXMLReader As Xml.XmlTextReader = New Xml.XmlTextReader(strMzMLFilePath)

			Do While objXMLReader.Read()
				XMLTextReaderSkipWhitespace(objXMLReader)
				If Not objXMLReader.ReadState = Xml.ReadState.Interactive Then Exit Do

				If objXMLReader.NodeType = Xml.XmlNodeType.Element Then
					Select Case objXMLReader.Name
						Case "spectrum"

							strSpectrumID = XMLTextReaderGetAttributeValue(objXMLReader, "id", String.Empty)

							If Not String.IsNullOrEmpty(strSpectrumID) Then
								If MsMsDataFileReader.clsMsMsDataFileReaderBaseClass.ExtractScanInfoFromDtaHeader(strSpectrumID, intScanNumberStart, intScanNumberEnd, intCharge) Then
									' This title is in a standard format
									udtScanInfo.ScanStart = intScanNumberStart
									udtScanInfo.ScanEnd = intScanNumberEnd
									udtScanInfo.Charge = intCharge
								Else
									blnAutoNumberScans = True
								End If

								If blnAutoNumberScans Then
									intScanNumberCurrent += 1
									udtScanInfo.ScanStart = intScanNumberCurrent
									udtScanInfo.ScanEnd = intScanNumberCurrent
									' Store a charge of 0 for now; we'll update it later if the selectedIon element has a MS:1000041 attribute
									udtScanInfo.Charge = 0
								Else
									intScanNumberCurrent = intScanNumberStart
								End If

							End If


						Case "selectedIon"
							' Read the cvParams for this selected ion							
							lstCVParams = GetCVParams(objXMLReader, "selectedIon")

							If lstCVParams.TryGetValue("MS:1000041", strValue) Then
								If Integer.TryParse(strValue, intValue) Then
									udtScanInfo.Charge = intValue
								End If
							End If
					End Select

				ElseIf objXMLReader.NodeType = Xml.XmlNodeType.EndElement AndAlso objXMLReader.Name = "spectrum" Then
					' Store this spectrum
					If Not String.IsNullOrEmpty(strSpectrumID) Then
						lstSpectrumIDToScanNumber.Add(strSpectrumID, udtScanInfo)
					End If
				End If
			Loop

		End Using

		Return True

	End Function

	Private Function XMLTextReaderGetAttributeValue(ByRef objXMLReader As Xml.XmlTextReader, ByVal strAttributeName As String, ByVal strValueIfMissing As String) As String
		objXMLReader.MoveToAttribute(strAttributeName)
		If objXMLReader.ReadAttributeValue() Then
			Return objXMLReader.Value
		Else
			Return String.Copy(strValueIfMissing)
		End If
	End Function

	Private Function XMLTextReaderGetInnerText(ByRef objXMLReader As Xml.XmlTextReader) As String
		Dim strValue As String = String.Empty
		Dim blnSuccess As Boolean

		If objXMLReader.NodeType = Xml.XmlNodeType.Element Then
			' Advance the reader so that we can read the value
			blnSuccess = objXMLReader.Read()
		Else
			blnSuccess = True
		End If

		If blnSuccess AndAlso Not objXMLReader.NodeType = Xml.XmlNodeType.Whitespace And objXMLReader.HasValue Then
			strValue = objXMLReader.Value
		End If

		Return strValue
	End Function

	Private Sub XMLTextReaderSkipWhitespace(ByRef objXMLReader As Xml.XmlTextReader)
		If objXMLReader.NodeType = Xml.XmlNodeType.Whitespace Then
			' Whitspace; read the next node
			objXMLReader.Read()
		End If
	End Sub

	Protected Function UpdateMGFFileTitleLinesUsingMzML(ByVal strMzMLFilePath As String, ByVal strMGFFilePath As String, ByVal strDatasetName As String) As Boolean

		Dim strNewMGFFile As String
		Dim strLineIn As String
		Dim strTitle As String

		Dim udtScanInfo As udtScanInfoType

		Dim blnSuccess As Boolean
		Dim blnAutoNumberScans As Boolean

		Dim lstSpectrumIDtoScanNumber As Dictionary(Of String, udtScanInfoType)
		lstSpectrumIDtoScanNumber = New Dictionary(Of String, udtScanInfoType)

		Try

			' Open the mzXML file and look for "spectrum" elements with an "id" attribute
			' Also look for the charge state

			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing the .mzML file to create the spectrum ID to scan number mapping")
			End If

			blnSuccess = ParseMzMLFile(strMzMLFilePath, blnAutoNumberScans, lstSpectrumIDtoScanNumber)

			If Not blnSuccess Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "ParseMzMLFile returned false; aborting")
				Return False
			ElseIf Not blnAutoNumberScans Then
				' Nothing to update; exit this function 
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Spectrum IDs in the mzML file were in the format StartScan.EndScan.Charge; no need to update the MGF file")
				Return True
			End If

			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Updating the Title lines in the MGF file")
			End If

			strNewMGFFile = Path.GetTempFileName

			' Now read the MGF file and update the title lines
			Using srSourceMGF As StreamReader = New StreamReader(New FileStream(strMGFFilePath, FileMode.Open, FileAccess.Read, FileShare.Read))
				Using swNewMGF As StreamWriter = New StreamWriter(New FileStream(strNewMGFFile, FileMode.Create, FileAccess.Write, FileShare.Read))

					Do While srSourceMGF.Peek > -1
						strLineIn = srSourceMGF.ReadLine()

						If String.IsNullOrEmpty(strLineIn) Then
							strLineIn = String.Empty
						Else

							If strLineIn.StartsWith("TITLE=") Then
								strTitle = strLineIn.Substring("TITLE=".Length())

								' Look for strTitle in lstSpectrumIDtoScanNumber
								If lstSpectrumIDtoScanNumber.TryGetValue(strTitle, udtScanInfo) Then
									strLineIn = "TITLE=" & strDatasetName & "." & udtScanInfo.ScanStart.ToString("0000") & "." & udtScanInfo.ScanEnd.ToString("0000") & "."
									If udtScanInfo.Charge > 0 Then
										' Also append charge
										strLineIn &= udtScanInfo.Charge
									End If
								End If
							End If
						End If

						swNewMGF.WriteLine(strLineIn)
					Loop

				End Using
			End Using

			If m_DebugLevel >= 1 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Update complete; replacing the original .MGF file")
			End If

			' Delete the original .mgf file and replace it with strNewMGFFile
			PRISM.Processes.clsProgRunner.GarbageCollectNow()
			Threading.Thread.Sleep(500)
			clsAnalysisToolRunnerBase.DeleteFileWithRetries(strMGFFilePath, m_DebugLevel)
			Threading.Thread.Sleep(500)

			Dim ioNewMGF As FileInfo = New FileInfo(strNewMGFFile)
			ioNewMGF.MoveTo(strMGFFilePath)

			blnSuccess = True

		Catch ex As Exception
			m_ErrMsg = "Error updating the MGF file title lines using the .mzML file"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_ErrMsg & ": " & ex.Message)
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	Private Sub mMGFtoDTA_ErrorEvent(strMessage As String) Handles mMGFtoDTA.ErrorEvent
		If String.IsNullOrEmpty(m_ErrMsg) Then
			m_ErrMsg = "MGFtoDTA_Error: " & strMessage
		ElseIf m_ErrMsg.Length < 300 Then
			m_ErrMsg &= "; MGFtoDTA_Error: " & strMessage
		End If

	End Sub
End Class
