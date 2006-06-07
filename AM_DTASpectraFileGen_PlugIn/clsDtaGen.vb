Imports System.Collections.Specialized
Imports PRISM.Processes
Imports PRISM.Logging
Imports PRISM.Files
Imports AnalysisManagerBase
Imports System.IO
Imports System.Text.RegularExpressions

Public Class clsDtaGen
	Implements ISpectraFileProcessor

	'This is the main class that implements a specific spectra file generator. It can be subclassed or combined with other classes in the 
	'	spectra file generator project as necessary.

#Region "Module variables"
	Protected m_DSName As String = ""	 'Handy place to store value so repeated calls to m_JobParams aren't required
	Protected m_ErrMsg As String = ""
	Protected m_MiscParams As StringDictionary
	Protected m_OutFolderPath As String = ""
	Protected m_SettingsFileName As String = ""	 'Handy place to store value so repeated calls to m_JobParams aren't required
	Protected m_SourceFolderPath As String = ""
	Protected m_Status As ISpectraFileProcessor.ProcessStatus
	Protected m_Results As ISpectraFileProcessor.ProcessResults
	Protected m_Settings As PRISM.Files.XmlSettingsFileAccessor
	Protected m_MgrParams As IMgrParams
	Protected m_JobParams As IJobParams
	Protected m_DebugLevel As Integer = 0
	Protected m_Logger As ILogger
	Protected m_SpectraFileCount As Integer
	Protected m_StatusTools As IStatusFile
#End Region

#Region "Properties"
	Public WriteOnly Property StatusTools() As IStatusFile Implements ISpectraFileProcessor.StatusTools
		Set(ByVal Value As IStatusFile)
			m_StatusTools = Value
		End Set
	End Property

	Public WriteOnly Property Logger() As ILogger Implements ISpectraFileProcessor.Logger
		Set(ByVal Value As ILogger)
			m_Logger = Value
		End Set
	End Property

	Public ReadOnly Property ErrMsg() As String Implements ISpectraFileProcessor.ErrMsg
		Get
			Return m_ErrMsg
		End Get
	End Property

	Public WriteOnly Property MiscParams() As StringDictionary Implements ISpectraFileProcessor.MiscParams
		Set(ByVal Value As StringDictionary)
			m_MiscParams = Value
		End Set
	End Property

	Public WriteOnly Property MgrParams() As IMgrParams Implements ISpectraFileProcessor.MgrParams
		Set(ByVal Value As IMgrParams)
			m_MgrParams = Value
		End Set
	End Property

	Public WriteOnly Property JobParams() As IJobParams Implements ISpectraFileProcessor.JobParams
		Set(ByVal Value As IJobParams)
			m_JobParams = Value
		End Set
	End Property

	Public Property OutputFolderPath() As String Implements ISpectraFileProcessor.OutputFolderPath
		Get
			Return m_OutFolderPath
		End Get
		Set(ByVal Value As String)
			m_OutFolderPath = Value
		End Set
	End Property

	Public Property SourceFolderPath() As String Implements ISpectraFileProcessor.SourceFolderPath
		Get
			Return m_SourceFolderPath
		End Get
		Set(ByVal Value As String)
			m_SourceFolderPath = Value
		End Set
	End Property

	Public ReadOnly Property Status() As ISpectraFileProcessor.ProcessStatus Implements ISpectraFileProcessor.Status
		Get
			Return m_Status
		End Get
	End Property

	Public ReadOnly Property Results() As ISpectraFileProcessor.ProcessResults Implements ISpectraFileProcessor.Results
		Get
			Return m_Results
		End Get
	End Property

	Public Property DebugLevel() As Integer Implements ISpectraFileProcessor.DebugLevel
		Get
			Return m_DebugLevel
		End Get
		Set(ByVal Value As Integer)
			m_DebugLevel = Value
		End Set
	End Property

	Public ReadOnly Property SpectraFileCount() As Integer Implements ISpectraFileProcessor.SpectraFileCount
		Get
			Return m_SpectraFileCount
		End Get
	End Property
#End Region

	Public Overridable Function Abort() As ISpectraFileProcessor.ProcessStatus Implements ISpectraFileProcessor.Abort

	End Function

	Public Overridable Function Start() As ISpectraFileProcessor.ProcessStatus Implements ISpectraFileProcessor.Start

	End Function

	Public Overridable Sub Setup(ByVal InitParams As ISpectraFileProcessor.InitializationParams) Implements ISpectraFileProcessor.Setup

		'Copies all input data required for plugin operation to appropriate memory variables
		With InitParams
			m_DebugLevel = .DebugLevel
			m_JobParams = .JobParams
			m_Logger = .Logger
			m_MgrParams = .MgrParams
			m_MiscParams = .MiscParams
			m_OutFolderPath = .OutputFolderPath
			m_SourceFolderPath = .SourceFolderPath
			m_StatusTools = .StatusTools
		End With

	End Sub

	Protected Overridable Function ReadSettingsFile(ByVal SettingsFile As String, ByVal WorkDir As String) As Boolean

		'Read the settings file

		If m_DebugLevel > 0 Then
			m_Logger.PostEntry("clsDtaGen.ReadSettingsFile: Reading settings file", _
			  PRISM.Logging.ILogger.logMsgType.logDebug, True)
		End If

		m_Settings = New PRISM.Files.XmlSettingsFileAccessor
		If m_Settings.LoadSettings(Path.Combine(WorkDir, SettingsFile)) Then
			Return True
		Else
			m_ErrMsg = "Unable to load settings file " & SettingsFile
			Return False
		End If

	End Function

	Protected Overridable Function VerifyDirExists(ByVal TestDir As String) As Boolean

		'Verifies that the specified directory exists
		If Directory.Exists(TestDir) Then
			m_ErrMsg = ""
			Return True
		Else
			m_ErrMsg = "Directory " & TestDir & " not found"
			Return False
		End If

	End Function

	Protected Overridable Function VerifyFileExists(ByVal TestFile As String) As Boolean
		'Verifies specified file exists
		If File.Exists(TestFile) Then
			m_ErrMsg = ""
			Return True
		Else
			m_ErrMsg = "File " & TestFile & " not found"
			Return False
		End If

	End Function

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
		Dim SettingsNamePath As String = Path.Combine(m_SourceFolderPath, m_SettingsFileName)
		If Not VerifyFileExists(SettingsNamePath) Then Return False 'Error msg handled by VerifyFileExists

		'If we got here, everything's OK
		Return True

	End Function

	Protected Overridable Function CountDtaFiles() As Integer

		'Returns the number of dta files in the working directory
		Dim FileList() As String = Directory.GetFiles(m_SourceFolderPath, "*.dta")
		Return FileList.GetLength(0)

	End Function

	Protected Overridable Function DeleteNonDosFiles() As Boolean

		'extract_msn.exe and lcq_dta.exe sometimes leave files with funky filenames containing non-DOS characters. This
		'	function removes those files

		Dim WorkDir As New DirectoryInfo(m_OutFolderPath)
		Dim TestFile As FileInfo
		Dim TestStr As String = ".dta$|.txt$|.csv$|.raw$|.params$|.wiff$|.xml$|.mgf$"

		For Each TestFile In WorkDir.GetFiles
			If Not Regex.IsMatch(TestFile.Extension, TestStr, RegexOptions.IgnoreCase) Then
				Try
					TestFile.Delete()
				Catch err As Exception
					m_ErrMsg = "Error removing non-DOS files"
					Return False
				End Try
			End If
		Next

		Return True

	End Function

End Class
