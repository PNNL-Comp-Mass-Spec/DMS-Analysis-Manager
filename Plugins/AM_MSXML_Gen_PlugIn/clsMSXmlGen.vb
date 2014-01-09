Option Strict On

Imports AnalysisManagerBase

Public MustInherit Class clsMSXmlGen

#Region "Enums"
	Public Enum MSXMLOutputTypeConstants
		mzXML = 0
		mzML = 1
	End Enum
#End Region

#Region "Module Variables"
	Protected mWorkDir As String
	Protected mProgramPath As String
	Protected mDatasetName As String
	Protected mRawDataType As clsAnalysisResources.eRawDataTypeConstants
	Protected mSourceFilePath As String = String.Empty
	Protected mOutputType As MSXMLOutputTypeConstants

	Protected mCentroidMS1 As Boolean
	Protected mCentroidMS2 As Boolean

	Protected mUseProgRunnerResultCode As Boolean		' When true, then return an error if the progrunner returns a non-zero exit code

	Protected mErrorMessage As String = String.Empty
	Protected mDebugLevel As Integer = 1

	Protected WithEvents CmdRunner As clsRunDosProgram

	Public Event ProgRunnerStarting(ByVal CommandLine As String)
	Public Event LoopWaiting()

#End Region

#Region "Properties"
	Public Property DebugLevel() As Integer
		Get
			Return mDebugLevel
		End Get
		Set(ByVal value As Integer)
			mDebugLevel = value
		End Set
	End Property

	Public ReadOnly Property ErrorMessage() As String
		Get
			If mErrorMessage Is Nothing Then
				Return String.Empty
			Else
				Return mErrorMessage
			End If
		End Get
	End Property

	Public ReadOnly Property SourceFilePath As String
		Get
			Return mSourceFilePath
		End Get
	End Property
#End Region


	Public Sub New(ByVal WorkDir As String,
	ByVal ProgramPath As String,
	ByVal DatasetName As String,
	ByVal RawDataType As clsAnalysisResources.eRawDataTypeConstants,
	ByVal eOutputType As MSXMLOutputTypeConstants,
	ByVal CentroidMSXML As Boolean)

		mWorkDir = WorkDir
		mProgramPath = ProgramPath
		mDatasetName = DatasetName
		mRawDataType = RawDataType
		mOutputType = eOutputType
		mCentroidMS1 = CentroidMSXML
		mCentroidMS2 = CentroidMSXML

		mErrorMessage = String.Empty
	End Sub

	Public Sub New(ByVal WorkDir As String,
	  ByVal ProgramPath As String,
	  ByVal DatasetName As String,
	  ByVal RawDataType As clsAnalysisResources.eRawDataTypeConstants,
	  ByVal eOutputType As MSXMLOutputTypeConstants,
	  ByVal CentroidMS1 As Boolean,
	  ByVal CentroidMS2 As Boolean)

		mWorkDir = WorkDir
		mProgramPath = ProgramPath
		mDatasetName = DatasetName
		mRawDataType = RawDataType
		mOutputType = eOutputType
		mCentroidMS1 = CentroidMS1
		mCentroidMS2 = CentroidMS2

		mErrorMessage = String.Empty
	End Sub

	Protected MustOverride Function CreateArguments(ByVal msXmlFormat As String, ByVal RawFilePath As String) As String

	''' <summary>
	''' Generate the mzXML or mzML file
	''' </summary>
	''' <returns>True if success; false if a failure</returns>
	''' <remarks></remarks>
	Public Function CreateMSXMLFile() As Boolean
		Dim CmdStr As String

		Dim msXmlFormat As String = "mzXML"

		Select Case mRawDataType
			Case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile
				mSourceFilePath = IO.Path.Combine(mWorkDir, mDatasetName & clsAnalysisResources.DOT_RAW_EXTENSION)
			Case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder
				mSourceFilePath = IO.Path.Combine(mWorkDir, mDatasetName & clsAnalysisResources.DOT_D_EXTENSION)
			Case Else
				Throw New ArgumentOutOfRangeException("Unsupported raw data type: " + mRawDataType.ToString())
		End Select


		Dim blnSuccess As Boolean

		mErrorMessage = String.Empty

		Select Case mOutputType
			Case MSXMLOutputTypeConstants.mzXML
				msXmlFormat = "mzXML"
			Case MSXMLOutputTypeConstants.mzML
				msXmlFormat = "mzML"
		End Select

		CmdRunner = New clsRunDosProgram(IO.Path.GetDirectoryName(mProgramPath))

		' Verify that program file exists
		If Not IO.File.Exists(mProgramPath) Then
			mErrorMessage = "Cannot find MSXmlGenerator exe program file: " & mProgramPath
			Return False
		End If

		'Set up and execute a program runner to run MS XML executable

		CmdStr = CreateArguments(msXmlFormat, mSourceFilePath)

		blnSuccess = SetupTool()
		If Not blnSuccess Then
			If String.IsNullOrEmpty(mErrorMessage) Then
				mErrorMessage = "SetupTool returned false"
			End If
			Return False
		End If


		RaiseEvent ProgRunnerStarting(mProgramPath & CmdStr)

		With CmdRunner
			.CreateNoWindow = True
			.CacheStandardOutput = True
			.EchoOutputToConsole = True

			.WriteConsoleOutputToFile = True
			.ConsoleOutputFilePath = IO.Path.Combine(mWorkDir, IO.Path.GetFileNameWithoutExtension(mProgramPath) & "_ConsoleOutput.txt")

			.WorkDir = mWorkDir
		End With

		blnSuccess = CmdRunner.RunProgram(mProgramPath, CmdStr, IO.Path.GetFileNameWithoutExtension(mProgramPath), mUseProgRunnerResultCode)

		If Not blnSuccess Then
			If CmdRunner.ExitCode <> 0 Then
				mErrorMessage = IO.Path.GetFileNameWithoutExtension(mProgramPath) & " returned a non-zero exit code: " & CmdRunner.ExitCode.ToString
				blnSuccess = False
			Else
				mErrorMessage = "Call to " & IO.Path.GetFileNameWithoutExtension(mProgramPath) & " failed (but exit code is 0)"
				blnSuccess = True
			End If
		Else
			' Make sure the output file was created and is non-zero
			Dim strOutputFilePath As String
			strOutputFilePath = IO.Path.ChangeExtension(mSourceFilePath, msXmlFormat)

			If Not IO.File.Exists(strOutputFilePath) Then
				mErrorMessage = "Output file not found: " & strOutputFilePath
				blnSuccess = False
			End If
		End If


		Return blnSuccess

	End Function

	Public Sub LogCreationStatsRawToMzXml(ByVal dtStartTimeUTC As DateTime, ByVal strWorkDirPath As String, ByVal strDatasetName As String)

		Dim strSourceFilePath As String = IO.Path.Combine(strWorkDirPath, strDatasetName & clsAnalysisResources.DOT_RAW_EXTENSION)
		Dim strMsXmlFilePath As String = IO.Path.Combine(strWorkDirPath, strDatasetName & clsAnalysisResources.DOT_MZXML_EXTENSION)

		LogCreationStatsSourceToMsXml(dtStartTimeUTC, strSourceFilePath, strMsXmlFilePath)

	End Sub

	Public Sub LogCreationStatsSourceToMsXml(ByVal dtStartTimeUTC As DateTime, ByVal strSourceFilePath As String, ByVal strMsXmlFilePath As String)

		Try
			' Save some stats to the log

			Dim strMessage As String
			Dim ioFileInfo As IO.FileInfo
			Dim dblSourceFileSizeMB As Double, dblMsXmlSizeMB As Double
			Dim dblTotalMinutes As Double

			Dim strSourceFileExtension As String = IO.Path.GetExtension(strSourceFilePath)
			Dim strTargetFileExtension As String = IO.Path.GetExtension(strMsXmlFilePath)

			dblTotalMinutes = DateTime.UtcNow.Subtract(dtStartTimeUTC).TotalMinutes

			ioFileInfo = New IO.FileInfo(strSourceFilePath)
			If ioFileInfo.Exists Then
				dblSourceFileSizeMB = ioFileInfo.Length / 1024.0 / 1024
			End If

			ioFileInfo = New IO.FileInfo(strMsXmlFilePath)
			If ioFileInfo.Exists Then
				dblMsXmlSizeMB = ioFileInfo.Length / 1024.0 / 1024
			End If

			strMessage = "MsXml creation time = " & dblTotalMinutes.ToString("0.00") & " minutes"

			If dblTotalMinutes > 0 Then
				strMessage &= "; Processing rate = " & (dblSourceFileSizeMB / dblTotalMinutes / 60).ToString("0.0") & " MB/second"
			End If

			strMessage &= "; " & strSourceFileExtension & " file size = " & dblSourceFileSizeMB.ToString("0.0") & " MB"
			strMessage &= "; " & strTargetFileExtension & " file size = " & dblMsXmlSizeMB.ToString("0.0") & " MB"

			If dblMsXmlSizeMB > 0 Then
				strMessage &= "; Filesize Ratio = " & (dblMsXmlSizeMB / dblSourceFileSizeMB).ToString("0.00")
			End If

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strMessage)
		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Exception saving msXML stats", ex)
		End Try

	End Sub

	Protected MustOverride Function SetupTool() As Boolean

	''' <summary>
	''' Event handler for CmdRunner.LoopWaiting event
	''' </summary>
	''' <remarks></remarks>
	Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
		RaiseEvent LoopWaiting()
	End Sub
End Class
