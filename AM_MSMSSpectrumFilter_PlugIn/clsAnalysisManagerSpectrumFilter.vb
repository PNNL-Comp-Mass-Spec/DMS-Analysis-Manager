Option Strict On

Imports AnalysisManagerBase

' This class implements the ISpectraFilter interface and can be 
' loaded as a pluggable DLL into the DMS Analysis Manager program.  It uses class
' clsMsMsSpectrumFilter to filter the .DTA files present in a given folder
'
' Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
' Copyright 2005, Battelle Memorial Institute
' Started October 13, 2005

Public Class clsAnalysisManagerSpectrumFilter
	Implements ISpectraFilter

#Region "Module variables"

	Protected m_SourceFolderPath As String = ""
	Protected m_OutFolderPath As String = ""

	Protected m_MiscParams As System.Collections.Specialized.StringDictionary
	Protected m_DebugLevel As Integer = 0
	Protected m_Logger As PRISM.Logging.ILogger
	Protected m_MgrParams As IMgrParams
	Protected m_JobParams As IJobParams
	Protected m_StatusTools As IStatusFile

	Protected m_DSName As String = ""								'Handy place to store value so repeated calls to m_JobParams aren't required
	Protected m_SettingsFileName As String = ""			'Handy place to store value so repeated calls to m_JobParams aren't required

	Protected m_ErrMsg As String = ""

	Protected m_Status As ISpectraFilter.ProcessStatus
	Protected m_Results As ISpectraFilter.ProcessResults

	Protected m_MsMsSpectrumFilter As clsMsMsSpectrumFilter

	Protected m_thThread As System.Threading.Thread

#End Region

#Region "Properties"
	Public ReadOnly Property ErrMsg() As String Implements ISpectraFilter.ErrMsg
		Get
			Return m_ErrMsg
		End Get
	End Property

	Public ReadOnly Property Results() As ISpectraFilter.ProcessResults Implements ISpectraFilter.Results
		Get
			Return m_Results
		End Get
	End Property

	Public ReadOnly Property Status() As ISpectraFilter.ProcessStatus Implements ISpectraFilter.Status
		Get
			Return m_Status
		End Get
	End Property

	Public ReadOnly Property SpectraFileCount() As Integer Implements ISpectraFilter.SpectraFileCount
		Get
			Return CountDtaFiles()
		End Get
	End Property
#End Region

	Public Function Abort() As ISpectraFilter.ProcessStatus Implements ISpectraFilter.Abort
		If Not m_MsMsSpectrumFilter Is Nothing Then
			m_Status = ISpectraFilter.ProcessStatus.SFILT_ABORTING
			m_MsMsSpectrumFilter.AbortProcessingNow()
		End If
	End Function

	Public Sub Setup(ByVal InitParams As ISpectraFilter.InitializationParams) Implements ISpectraFilter.Setup

		'Copies all input data required for plugin operation to appropriate memory variables
		With InitParams
			m_SourceFolderPath = .SourceFolderPath
			m_OutFolderPath = .OutputFolderPath

			m_MiscParams = .MiscParams
			m_DebugLevel = .DebugLevel
			m_Logger = .Logger
			m_MgrParams = .MgrParams
			m_JobParams = .JobParams

			m_StatusTools = .StatusTools
		End With

	End Sub

	Public Function Start() As ISpectraFilter.ProcessStatus Implements ISpectraFilter.Start

		m_Status = ISpectraFilter.ProcessStatus.SFILT_STARTING

		'Verify necessary files are in specified locations
		If Not InitSetup() Then
			m_Results = ISpectraFilter.ProcessResults.SFILT_FAILURE
			m_Status = ISpectraFilter.ProcessStatus.SFILT_ERROR
			Return m_Status
		End If

		' Filter the spectra (the process runs in a separate thread)
		m_Status = FilterDTAFilesInFolder()

		If m_Status = ISpectraFilter.ProcessStatus.SFILT_ERROR Then
			m_Results = ISpectraFilter.ProcessResults.SFILT_FAILURE
		End If

		Return m_Status

	End Function

	Protected Overridable Function CountDtaFiles() As Integer

		'Returns the number of dta files in the working directory
		Return System.IO.Directory.GetFiles(m_SourceFolderPath, "*.dta").Length

	End Function

	Protected Overridable Function FilterDTAFilesInFolder() As ISpectraFilter.ProcessStatus
		' Initializes m_MsMsSpectrumFilter, then starts a separate thread 
		' to filter the DTA files in the working folder

		Dim strParameterFilePath As String

		Try
			'Initialize MsMsSpectrumFilterDLL.dll
			m_MsMsSpectrumFilter = New clsMsMsSpectrumFilter

			' Pre-read the parameter file now, so that we don't re-read it for each file
			' found via .ProcessFilesWildcard()
			strParameterFilePath = System.IO.Path.Combine(m_SourceFolderPath, m_SettingsFileName)
			If Not m_MsMsSpectrumFilter.LoadParameterFileSettings(strParameterFilePath) Then
				m_ErrMsg = m_MsMsSpectrumFilter.GetErrorMessage
				If m_ErrMsg Is Nothing OrElse m_ErrMsg.Length = 0 Then
					m_ErrMsg = "Parameter file load error: " & strParameterFilePath
				End If
				LogErrors("FilterDTAFilesInFolder", m_ErrMsg, Nothing)
				Return ISpectraFilter.ProcessStatus.SFILT_ERROR
			End If

			' Set a few additional settings
			With m_MsMsSpectrumFilter
				.DeleteBadDTAFiles = True

				.OverwriteReportFile = True
				.AutoCloseReportFile = False
			End With

			m_thThread = New System.Threading.Thread(AddressOf FilterDTAFilesWork)
			m_thThread.Start()

			m_Status = ISpectraFilter.ProcessStatus.SFILT_RUNNING

		Catch ex As Exception
			LogErrors("FilterDTAFilesInFolder", "Error initializing and running clsMsMsSpectrumFilter", ex)
			m_Status = ISpectraFilter.ProcessStatus.SFILT_ERROR
		End Try

		Return m_Status
	End Function

	Protected Overridable Sub FilterDTAFilesWork()

		Dim strInputFileSpec As String

		Dim blnSuccess As Boolean

		strInputFileSpec = System.IO.Path.Combine(m_SourceFolderPath, "*.dta")
		blnSuccess = m_MsMsSpectrumFilter.ProcessFilesWildcard(strInputFileSpec, m_OutFolderPath, "")

		If blnSuccess Then
			' Sort the report file (this also closes the file)
			m_MsMsSpectrumFilter.SortSpectrumQualityTextFile()

			'Count the number of .Dta files remaining
			If Not VerifyDtaCreation() Then
				m_Results = ISpectraFilter.ProcessResults.SFILT_NO_FILES_CREATED
			Else
				m_Results = ISpectraFilter.ProcessResults.SFILT_SUCCESS
			End If

			m_Status = ISpectraFilter.ProcessStatus.SFILT_COMPLETE
		Else
			If m_MsMsSpectrumFilter.AbortProcessing Then
				LogErrors("FilterDTAFilesWork", "Processing aborted", Nothing)
				m_Results = ISpectraFilter.ProcessResults.SFILT_ABORTED
				m_Status = ISpectraFilter.ProcessStatus.SFILT_ABORTING
			Else
				LogErrors("FilterDTAFilesWork", m_MsMsSpectrumFilter.GetErrorMessage(), Nothing)
				m_Results = ISpectraFilter.ProcessResults.SFILT_FAILURE
				m_Status = ISpectraFilter.ProcessStatus.SFILT_ERROR
			End If
		End If

	End Sub

	Protected Overridable Function InitSetup() As Boolean

		'Initializes module variables and verifies mandatory parameters have been propery specified

		'Manager parameters
		If m_MgrParams Is Nothing Then
			m_ErrMsg = "Manager parameters not specified"
			Return False
		End If

		'Job parameters
		If m_JobParams Is Nothing Then
			m_ErrMsg = "Job parameters not specified"
			Return False
		End If

		'Logger
		If m_Logger Is Nothing Then
			m_ErrMsg = "Logging object not set"
			Return False
		End If

		'Status tools
		If m_StatusTools Is Nothing Then
			m_ErrMsg = "Status tools object not set"
			Return False
		End If

		'Set dataset name
		m_DSName = m_JobParams.GetParam("datasetNum")

		'Set settings file name
		m_SettingsFileName = m_JobParams.GetParam("settingsFileName")

		'Output folder name
		If m_OutFolderPath = "" Then
			m_ErrMsg = "Output folder path not specified"
			Return False
		End If

		'Source folder name
		If m_SourceFolderPath = "" Then
			m_ErrMsg = "Source folder not specified"
			Return False
		End If

		'Source directory exist?
		If Not VerifyDirExists(m_SourceFolderPath) Then Return False 'Error msg handled by VerifyDirExists

		'Output directory exist?
		If Not VerifyDirExists(m_OutFolderPath) Then Return False 'Error msg handled by VerifyDirExists

		'Settings file exist?
		Dim SettingsNamePath As String = System.IO.Path.Combine(m_SourceFolderPath, m_SettingsFileName)
		If Not VerifyFileExists(SettingsNamePath) Then Return False 'Error msg handled by VerifyFileExists

		'If we got here, everything's OK
		Return True

	End Function

	Private Sub LogErrors(ByVal strSource As String, ByVal strMessage As String, ByVal ex As Exception, Optional ByVal blnLogLocalOnly As Boolean = True)

		m_ErrMsg = String.Copy(strMessage).Replace(ControlChars.NewLine, "; ")

		If ex Is Nothing Then
			ex = New System.Exception("Error")
		Else
			If Not ex.Message Is Nothing AndAlso ex.Message.Length > 0 Then
				m_ErrMsg &= "; " & ex.Message
			End If
		End If

		Trace.WriteLine(Now.ToLongTimeString & "; " & m_ErrMsg, strSource)
		Console.WriteLine(Now.ToLongTimeString & "; " & m_ErrMsg, strSource)

		If Not m_Logger Is Nothing Then
			m_Logger.PostError(m_ErrMsg, ex, blnLogLocalOnly)
		End If

	End Sub

	Protected Overridable Function VerifyDirExists(ByVal TestDir As String) As Boolean

		'Verifies that the specified directory exists
		If System.IO.Directory.Exists(TestDir) Then
			m_ErrMsg = ""
			Return True
		Else
			m_ErrMsg = "Directory " & TestDir & " not found"
			Return False
		End If

	End Function

	Private Function VerifyDtaCreation() As Boolean

		Dim DtaFiles() As String

		'Verify at least one .dta file has been created
		If CountDtaFiles() < 1 Then
			m_ErrMsg = "No dta files remain after filtering"
			Return False
		Else
			Return True
		End If

	End Function

	Protected Overridable Function VerifyFileExists(ByVal TestFile As String) As Boolean
		'Verifies specified file exists
		If System.IO.File.Exists(TestFile) Then
			m_ErrMsg = ""
			Return True
		Else
			m_ErrMsg = "File " & TestFile & " not found"
			Return False
		End If

	End Function
End Class
