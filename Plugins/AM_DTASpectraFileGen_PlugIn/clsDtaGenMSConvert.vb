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

	Private Sub mMGFtoDTA_ErrorEvent(strMessage As String) Handles mMGFtoDTA.ErrorEvent
		If String.IsNullOrEmpty(m_ErrMsg) Then
			m_ErrMsg = "MGFtoDTA_Error: " & strMessage
		ElseIf m_ErrMsg.Length < 300 Then
			m_ErrMsg &= "; MGFtoDTA_Error: " & strMessage
		End If
	End Sub

End Class
