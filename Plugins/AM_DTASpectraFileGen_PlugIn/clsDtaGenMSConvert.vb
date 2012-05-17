'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 04/12/2012
'
' Uses MSConvert to create a .MGF file from a .Raw file or .mzXML file or .mzML file
' Next, converts the .MGF file to a _DTA.txt file
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsDtaGenMSConvert
	Inherits clsDtaGenThermoRaw

	Public Const DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN As Integer = 250

	Protected WithEvents mMGFtoDTA As MascotGenericFileToDTA.clsMGFtoDTA

	Protected Structure udtScanInfoType
		Public ScanStart As Integer
		Public ScanEnd As Integer
		Public Charge As Integer
	End Structure

	Public Overrides Sub Setup(InitParams As AnalysisManagerBase.ISpectraFileProcessor.InitializationParams)
		MyBase.Setup(InitParams)

		' Tool setup for MSConvert involves creating a
		'  registry entry at HKEY_CURRENT_USER\Software\ProteoWizard
		'  to indicate that we agree to the Thermo license

		Dim objProteowizardTools As clsProteowizardTools
		objProteowizardTools = New clsProteowizardTools(m_DebugLevel)

		If Not objProteowizardTools.RegisterProteoWizard() Then
			Throw New Exception("Unable to register ProteoWizard")
		End If

	End Sub

	Protected Overrides Function ConstructDTAToolPath() As String

		Dim strDTAToolPath As String

		Dim ProteoWizardDir As String = m_MgrParams.GetParam("ProteoWizardDir")			' MSConvert.exe is stored in the ProteoWizard folder
		strDTAToolPath = System.IO.Path.Combine(ProteoWizardDir, MSCONVERT_FILENAME)

		Return strDTAToolPath

	End Function

	Protected Overrides Sub MakeDTAFilesThreaded()

		m_Status = ISpectraFileProcessor.ProcessStatus.SF_RUNNING
		m_ErrMsg = String.Empty

		m_Progress = 10

		If Not ConvertRawToMGF(m_RawDataType) Then
			If m_Status <> ISpectraFileProcessor.ProcessStatus.SF_ABORTING Then
				m_Results = ISpectraFileProcessor.ProcessResults.SF_FAILURE
				m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
			End If
			Return
		End If

		m_Progress = 75

		If Not ConvertMGFtoDTA() Then
			If m_Status <> ISpectraFileProcessor.ProcessStatus.SF_ABORTING Then
				m_Results = ISpectraFileProcessor.ProcessResults.SF_FAILURE
				m_Status = ISpectraFileProcessor.ProcessStatus.SF_ERROR
			End If
			Return
		End If

		m_Results = ISpectraFileProcessor.ProcessResults.SF_SUCCESS
		m_Status = ISpectraFileProcessor.ProcessStatus.SF_COMPLETE

	End Sub

	''' <summary>
	''' Convert .mgf file to _DTA.txt using MascotGenericFileToDTA.dll
	''' This functon is called by MakeDTAFilesThreaded
	''' </summary>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Private Function ConvertMGFtoDTA() As Boolean

		Dim strMGFFilePath As String
		Dim blnSuccess As Boolean

		If m_DebugLevel > 0 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Converting .MGF file to _DTA.txt")
		End If

		strMGFFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & ".mgf")

		Dim strRawDataType As String = m_JobParams.GetParam("RawDataType")
		Dim eRawDataType As clsAnalysisResources.eRawDataTypeConstants

		eRawDataType = clsAnalysisResources.GetRawDataType(strRawDataType)

		If eRawDataType = clsAnalysisResources.eRawDataTypeConstants.mzML Then
			' Read the .mzML file to construct a mapping between "title" line and scan number
			' If necessary, update the .mgf file to have new "title" lines that clsMGFtoDTA will recognize

			Dim strMzMLFilePath As String
			strMzMLFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZML_EXTENSION)

			blnSuccess = UpdateMGFFileTitleLinesUsingMzML(strMzMLFilePath, strMGFFilePath)
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

		m_SpectraFileCount = mMGFtoDTA.SpectraCountWritten
		m_Progress = 95

		Return blnSuccess

	End Function


	''' <summary>
	''' Create .mgf file using MSConvert
	''' This functon is called by MakeDTAFilesThreaded
	''' </summary>
	''' <param name="eRawDataType">Raw data file type</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Private Function ConvertRawToMGF(ByVal eRawDataType As clsAnalysisResources.eRawDataTypeConstants) As Boolean

		Dim CmdStr As String
		Dim RawFilePath As String

		Dim ScanStart As Integer
		Dim ScanStop As Integer

		Dim CentroidMGF As Boolean
		Dim CentroidPeakCountToRetain As Integer

		If m_DebugLevel > 0 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Creating .MGF file using MSConvert")
		End If

		' Construct the path to the .raw file
		Select Case eRawDataType
			Case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile
				RawFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_RAW_EXTENSION)
			Case clsAnalysisResources.eRawDataTypeConstants.mzXML
				RawFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZXML_EXTENSION)
			Case clsAnalysisResources.eRawDataTypeConstants.mzML
				RawFilePath = System.IO.Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZML_EXTENSION)
			Case Else
				m_ErrMsg = "Raw data file type not supported: " & eRawDataType.ToString()
				Return False
		End Select

		m_JobParams.AddResultFileToSkip(System.IO.Path.GetFileName(RawFilePath))

		ScanStart = 1
		ScanStop = 999999

		If eRawDataType = clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile Then
			'Get the maximum number of scans in the file
			m_MaxScanInFile = GetMaxScan(RawFilePath)
		Else
			m_MaxScanInFile = ScanStop
		End If

		Select Case m_MaxScanInFile
			Case -1
				' Generic error getting number of scans
				m_ErrMsg = "Unknown error getting number of scans; Maxscan = " & m_MaxScanInFile.ToString
				Return False
			Case 0
				' Unable to read file; treat this is a warning
				m_ErrMsg = "Warning: unable to get maxscan; Maxscan = 0"
			Case Is > 0
				' This is normal, do nothing
			Case Else
				' This should never happen
				m_ErrMsg = "Critical error getting number of scans; Maxscan = " & m_MaxScanInFile.ToString
				Return False
		End Select

		'Verify max scan specified is in file
		If m_MaxScanInFile > 0 Then
			If ScanStop > m_MaxScanInFile Then ScanStop = m_MaxScanInFile
		End If

		'Determine max number of scans to be performed
		m_NumScans = ScanStop - ScanStart + 1

		'Setup a program runner tool to make the spectra files
		m_RunProgTool = New clsRunDosProgram(m_WorkDir)

		' Lookup Centroid Settings
		CentroidMGF = m_JobParams.GetJobParameter("CentroidMGF", False)
		CentroidPeakCountToRetain = m_JobParams.GetJobParameter("CentroidPeakCountToRetain", DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN)

		'Set up command
		CmdStr = " " & RawFilePath

		If CentroidMGF Then
			' Centroid the data by first applying the peak-picking algorithm, then keeping the top N data points
			' Syntax details:
			'   peakPicking prefer_vendor:<true|false>  int_set(MS levels)
			'   threshold <count|count-after-ties|absolute|bpi-relative|tic-relative|tic-cutoff> <threshold> <most-intense|least-intense> [int_set(MS levels)]

			' So, the following means to apply peak picking to all spectra (MS1 and MS2) and then keep the top 250 peaks (sorted by intensity)
			' --filter "peakPicking true 1-" --filter "threshold count 250 most-intense"

			If CentroidPeakCountToRetain = 0 Then
				CentroidPeakCountToRetain = DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN
			ElseIf CentroidPeakCountToRetain < 25 Then
				CentroidPeakCountToRetain = 25
			End If

			CmdStr &= " --filter ""peakPicking true 1-"" --filter ""threshold count " & CentroidPeakCountToRetain & " most-intense"""
		End If

		CmdStr &= " --mgf -o " & m_WorkDir

		If m_DebugLevel > 0 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_DtaToolNameLoc & " " & CmdStr)
		End If

		With m_RunProgTool
			.CreateNoWindow = True
			.CacheStandardOutput = True
			.EchoOutputToConsole = True

			.WriteConsoleOutputToFile = True
			.ConsoleOutputFilePath = String.Empty	   ' Allow the console output filename to be auto-generated
		End With

		If Not m_RunProgTool.RunProgram(m_DtaToolNameLoc, CmdStr, "MSConvert", True) Then
			' .RunProgram returned False
			LogDTACreationStats("ConvertRawToMGF", System.IO.Path.GetFileNameWithoutExtension(m_DtaToolNameLoc), "m_RunProgTool.RunProgram returned False")

			m_ErrMsg = "Error running " & System.IO.Path.GetFileNameWithoutExtension(m_DtaToolNameLoc)
			Return False
		End If

		If m_DebugLevel >= 2 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... MGF file created")
		End If

		Return True

	End Function

	Protected Function GetCVParams(ByRef objXMLReader As System.Xml.XmlTextReader, ByVal strCurrentElementName As String) As System.Collections.Generic.Dictionary(Of String, String)

		Dim lstCVParams As System.Collections.Generic.Dictionary(Of String, String) = New System.Collections.Generic.Dictionary(Of String, String)()
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

	Protected Function ParseMzMLFile(ByVal strMzMLFilePath As String, ByRef blnAutoNumberScans As Boolean, lstSpectrumIDToScanNumber As System.Collections.Generic.Dictionary(Of String, udtScanInfoType)) As Boolean

		Dim strSpectrumID As String = String.Empty

		Dim intScanNumberStart As Integer
		Dim intScanNumberEnd As Integer
		Dim intCharge As Integer
		Dim udtScanInfo As udtScanInfoType

		Dim intScanNumberCurrent As Integer = 0
		Dim strValue As String = String.Empty
		Dim intValue As Integer

		Dim lstCVParams As System.Collections.Generic.Dictionary(Of String, String)

		blnAutoNumberScans = False

		If lstSpectrumIDToScanNumber Is Nothing Then
			lstSpectrumIDToScanNumber = New System.Collections.Generic.Dictionary(Of String, udtScanInfoType)
		End If

		Using objXMLReader As System.Xml.XmlTextReader = New System.Xml.XmlTextReader(strMzMLFilePath)

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

	Protected Function UpdateMGFFileTitleLinesUsingMzML(ByVal strMzMLFilePath As String, ByVal strMGFFilePath As String) As Boolean

		Dim strNewMGFFile As String
		Dim strLineIn As String
		Dim strTitle As String

		Dim udtScanInfo As udtScanInfoType

		Dim blnSuccess As Boolean
		Dim blnAutoNumberScans As Boolean

		Dim lstSpectrumIDtoScanNumber As System.Collections.Generic.Dictionary(Of String, udtScanInfoType)
		lstSpectrumIDtoScanNumber = New System.Collections.Generic.Dictionary(Of String, udtScanInfoType)

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

			strNewMGFFile = System.IO.Path.GetTempFileName

			' Now read the MGF file and update the title lines
			Using srSourceMGF As System.IO.StreamReader = New System.IO.StreamReader(New System.IO.FileStream(strMGFFilePath, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.Read))
				Using swNewMGF As System.IO.StreamWriter = New System.IO.StreamWriter(New System.IO.FileStream(strNewMGFFile, IO.FileMode.Create, IO.FileAccess.Write, IO.FileShare.Read))

					Do While srSourceMGF.Peek > -1
						strLineIn = srSourceMGF.ReadLine()

						If String.IsNullOrEmpty(strLineIn) Then
							strLineIn = String.Empty
						Else

							If strLineIn.StartsWith("TITLE=") Then
								strTitle = strLineIn.Substring("TITLE=".Length())

								' Look for strTitle in lstSpectrumIDtoScanNumber
								If lstSpectrumIDtoScanNumber.TryGetValue(strTitle, udtScanInfo) Then
									strLineIn = "TITLE=" & m_Dataset & "." & udtScanInfo.ScanStart.ToString("0000") & "." & udtScanInfo.ScanEnd.ToString("0000") & "."
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
			System.Threading.Thread.Sleep(500)
			clsAnalysisToolRunnerBase.DeleteFileWithRetries(strMGFFilePath, m_DebugLevel)
			System.Threading.Thread.Sleep(500)

			Dim ioNewMGF As System.IO.FileInfo = New System.IO.FileInfo(strNewMGFFile)
			ioNewMGF.MoveTo(strMGFFilePath)

			blnSuccess = True

		Catch ex As Exception
			m_ErrMsg = "Error updating the MGF file title lines using the .mzML file"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_ErrMsg & ": " & ex.Message)
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function


	Private Function XMLTextReaderGetAttributeValue(ByRef objXMLReader As System.Xml.XmlTextReader, ByVal strAttributeName As String, ByVal strValueIfMissing As String) As String
		objXMLReader.MoveToAttribute(strAttributeName)
		If objXMLReader.ReadAttributeValue() Then
			Return objXMLReader.Value
		Else
			Return String.Copy(strValueIfMissing)
		End If
	End Function

	Private Function XMLTextReaderGetInnerText(ByRef objXMLReader As System.Xml.XmlTextReader) As String
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

	Private Sub XMLTextReaderSkipWhitespace(ByRef objXMLReader As System.Xml.XmlTextReader)
		If objXMLReader.NodeType = Xml.XmlNodeType.Whitespace Then
			' Whitspace; read the next node
			objXMLReader.Read()
		End If
	End Sub

	Private Sub mMGFtoDTA_ErrorEvent(strMessage As String) Handles mMGFtoDTA.ErrorEvent
		If String.IsNullOrEmpty(m_ErrMsg) Then
			m_ErrMsg = "MGFtoDTA_Error: " & strMessage
		ElseIf m_ErrMsg.Length < 300 Then
			m_ErrMsg &= "; MGFtoDTA_Error: " & strMessage
		End If
	End Sub

End Class
