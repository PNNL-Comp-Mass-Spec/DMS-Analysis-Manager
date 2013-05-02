Option Strict On

'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
'
' This class is intended to be instantiated by other Analysis Manager plugins
' For example, see AM_MSGF_PlugIn
'
'*********************************************************************************************************

Imports AnalysisManagerBase

Public Class clsMSXMLCreator

#Region "Classwide variables"
	Protected mMSXmlGeneratorAppPath As String
	Protected m_jobParams As IJobParams
	Protected m_WorkDir As String
	Protected m_Dataset As String
	Protected m_DebugLevel As Short

	Protected m_ErrorMessage As String

	Protected WithEvents mMSXmlGen As clsMSXmlGen

	Public Event DebugEvent(ByVal Message As String)
	Public Event ErrorEvent(ByVal Message As String)
	Public Event WarningEvent(ByVal Message As String)

	Public Event LoopWaiting()

#End Region

	Public ReadOnly Property ErrorMessage() As String
		Get
			Return m_ErrorMessage
		End Get
	End Property

	Public Sub New(ByVal MSXmlGeneratorAppPath As String, ByVal WorkDir As String, ByVal Dataset As String, ByVal DebugLevel As Short, ByVal JobParams As IJobParams)

		mMSXmlGeneratorAppPath = MSXmlGeneratorAppPath
		m_WorkDir = WorkDir
		m_Dataset = Dataset
		m_DebugLevel = DebugLevel
		m_jobParams = JobParams

		m_ErrorMessage = String.Empty

	End Sub

	Public Function ConvertMzMLToMzXML() As Boolean

		Dim oProgRunner As clsRunDosProgram
		Dim ProgLoc As String
		Dim CmdStr As String

		Dim dtStartTimeUTC As System.DateTime
		Dim strSourceFilePath As String

		' mzXML filename is dataset plus .mzXML
		Dim strMzXmlFilePath As String
		strMzXmlFilePath = IO.Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZXML_EXTENSION)

		If IO.File.Exists(strMzXmlFilePath) OrElse IO.File.Exists(strMzXmlFilePath & clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX) Then
			' File already exists; nothing to do
			Return True
		End If

		strSourceFilePath = IO.Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZML_EXTENSION)

		ProgLoc = mMSXmlGeneratorAppPath
		If Not IO.File.Exists(ProgLoc) Then
			m_ErrorMessage = "MSXmlGenerator not found; unable to convert .mzML file to .mzXML"
			ReportError(m_ErrorMessage & ": " & mMSXmlGeneratorAppPath)
			Return False
		End If

		If m_DebugLevel >= 2 Then
			ReportDebugInfo("Creating the .mzXML file for " & m_Dataset & " using " & IO.Path.GetFileName(strSourceFilePath))
		End If

		'Setup a program runner tool to call MSConvert
		oProgRunner = New clsRunDosProgram(m_WorkDir)

		'Set up command
		CmdStr = " " & clsAnalysisToolRunnerBase.PossiblyQuotePath(strSourceFilePath) & " --mzXML -o " & m_WorkDir

		If m_DebugLevel > 0 Then
			ReportDebugInfo(ProgLoc & " " & CmdStr)
		End If

		With oProgRunner
			.CreateNoWindow = True
			.CacheStandardOutput = True
			.EchoOutputToConsole = True

			.WriteConsoleOutputToFile = False
			.ConsoleOutputFilePath = String.Empty	   ' Allow the console output filename to be auto-generated
		End With


		dtStartTimeUTC = System.DateTime.UtcNow

		If Not oProgRunner.RunProgram(ProgLoc, CmdStr, "MSConvert", True) Then
			' .RunProgram returned False
			m_ErrorMessage = "Error running " & IO.Path.GetFileNameWithoutExtension(ProgLoc) & " to convert the .mzML file to a .mzXML file"
			ReportError(m_ErrorMessage)
			Return False
		End If

		If m_DebugLevel >= 2 Then
			ReportDebugInfo(" ... mzXML file created")
		End If

		' Validate that the .mzXML file was actually created
		If Not IO.File.Exists(strMzXmlFilePath) Then
			m_ErrorMessage = ".mzXML file was not created by MSConvert"
			ReportError(m_ErrorMessage & ": " & strMzXmlFilePath)
			Return False
		End If

		If m_DebugLevel >= 1 Then
			mMSXmlGen.LogCreationStatsSourceToMsXml(dtStartTimeUTC, strSourceFilePath, strMzXmlFilePath)
		End If

		Return True

	End Function

	''' <summary>
	''' Generate the mzXML
	''' </summary>
	''' <returns>True if success; false if an error</returns>
	''' <remarks></remarks>
	Public Function CreateMZXMLFile() As Boolean

		Dim dtStartTimeUTC As System.DateTime

		' Turn on Centroiding, which will result in faster mzXML file generation time and smaller .mzXML files
		Dim CentroidMSXML As Boolean = True

		Dim eOutputType As clsMSXmlGen.MSXMLOutputTypeConstants

		Dim blnSuccess As Boolean

		' mzXML filename is dataset plus .mzXML
		Dim strMzXmlFilePath As String
		strMzXmlFilePath = IO.Path.Combine(m_WorkDir, m_Dataset & clsAnalysisResources.DOT_MZXML_EXTENSION)

		If IO.File.Exists(strMzXmlFilePath) OrElse IO.File.Exists(strMzXmlFilePath & clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX) Then
			' File already exists; nothing to do
			Return True
		End If

		eOutputType = clsMSXmlGen.MSXMLOutputTypeConstants.mzXML

		' Instantiate the processing class
		' Note that mMSXmlGeneratorAppPath should have been populated by StoreToolVersionInfo() by an Analysis Manager plugin using clsAnalysisToolRunnerBase.GetMSXmlGeneratorAppPath()
		Dim strMSXmlGeneratorExe As String
		strMSXmlGeneratorExe = IO.Path.GetFileName(mMSXmlGeneratorAppPath)

		If Not IO.File.Exists(mMSXmlGeneratorAppPath) Then
			m_ErrorMessage = "MSXmlGenerator not found; unable to create .mzXML file"
			ReportError(m_ErrorMessage & ": " & mMSXmlGeneratorAppPath)
			Return False
		End If


		If m_DebugLevel >= 2 Then
			ReportDebugInfo("Creating the .mzXML file for " & m_Dataset)
		End If

		If strMSXmlGeneratorExe.ToLower().Contains("readw") Then
			' ReadW
			' mMSXmlGeneratorAppPath should have been populated during the call to StoreToolVersionInfo()

			mMSXmlGen = New clsMSXMLGenReadW(m_WorkDir, mMSXmlGeneratorAppPath, m_Dataset, eOutputType, CentroidMSXML)

		ElseIf strMSXmlGeneratorExe.ToLower().Contains("msconvert") Then
			' MSConvert

			' Lookup Centroid Settings
			CentroidMSXML = m_jobParams.GetJobParameter("CentroidMSXML", True)
			Dim CentroidPeakCountToRetain As Integer

			' Look for parameter CentroidPeakCountToRetain in the MSXMLGenerator section
			CentroidPeakCountToRetain = m_jobParams.GetJobParameter("MSXMLGenerator", "CentroidPeakCountToRetain", 0)

			If CentroidPeakCountToRetain = 0 Then
				' Look for parameter CentroidPeakCountToRetain in any section
				CentroidPeakCountToRetain = m_jobParams.GetJobParameter("CentroidPeakCountToRetain", clsMSXmlGenMSConvert.DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN)
			End If

			mMSXmlGen = New clsMSXmlGenMSConvert(m_WorkDir, mMSXmlGeneratorAppPath, m_Dataset, eOutputType, CentroidMSXML, CentroidPeakCountToRetain)

		Else
			m_ErrorMessage = "Unsupported XmlGenerator: " & strMSXmlGeneratorExe
			ReportError(m_ErrorMessage)
			Return False
		End If

		dtStartTimeUTC = System.DateTime.UtcNow

		' Create the file
		blnSuccess = mMSXmlGen.CreateMSXMLFile()

		If Not blnSuccess Then
			m_ErrorMessage = mMSXmlGen.ErrorMessage
			ReportError(mMSXmlGen.ErrorMessage)
			Return False

		ElseIf mMSXmlGen.ErrorMessage.Length > 0 Then
			ReportWarning(mMSXmlGen.ErrorMessage)
		End If

		' Validate that the .mzXML file was actually created
		If Not IO.File.Exists(strMzXmlFilePath) Then
			m_ErrorMessage = ".mzXML file was not created by " & strMSXmlGeneratorExe
			ReportError(m_ErrorMessage & ": " & strMzXmlFilePath)
			Return False
		End If

		If m_DebugLevel >= 1 Then
			mMSXmlGen.LogCreationStatsSourceToMsXml(dtStartTimeUTC, mMSXmlGen.SourceFilePath, strMzXmlFilePath)
		End If

		Return True

	End Function

	Protected Sub ReportDebugInfo(ByVal Message As String)
		RaiseEvent DebugEvent(Message)
	End Sub

	Protected Sub ReportError(ByVal Message As String)
		RaiseEvent ErrorEvent(Message)
	End Sub

	Protected Sub ReportWarning(ByVal Message As String)
		RaiseEvent WarningEvent(Message)
	End Sub

	Public Sub UpdateDatasetName(ByVal DatasetName As String)
		m_Dataset = DatasetName
	End Sub

#Region "Event Handlers"

	''' <summary>
	''' Event handler for MSXmlGenReadW.LoopWaiting event
	''' </summary>
	''' <remarks></remarks>
	Private Sub MSXmlGenReadW_LoopWaiting() Handles mMSXmlGen.LoopWaiting

		RaiseEvent LoopWaiting()

	End Sub

	''' <summary>
	''' Event handler for mMSXmlGen.ProgRunnerStarting event
	''' </summary>
	''' <param name="CommandLine">The command being executed (program path plus command line arguments)</param>
	''' <remarks></remarks>
	Private Sub mMSXmlGenReadW_ProgRunnerStarting(ByVal CommandLine As String) Handles mMSXmlGen.ProgRunnerStarting
		If m_DebugLevel >= 2 Then
			ReportDebugInfo(CommandLine)
		End If
	End Sub

#End Region

End Class
