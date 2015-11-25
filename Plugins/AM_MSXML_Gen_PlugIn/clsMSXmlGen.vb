Option Strict On

Imports AnalysisManagerBase
Imports System.IO

Public MustInherit Class clsMSXmlGen

#Region "Constants"
    ' Define a maximum runtime of 36 hours
    Const MAX_RUNTIME_SECONDS As Integer = 36 * 60 * 60
#End Region

#Region "Module Variables"
	Protected ReadOnly mWorkDir As String
	Protected ReadOnly mProgramPath As String
	Protected ReadOnly mDatasetName As String
	Protected ReadOnly mRawDataType As clsAnalysisResources.eRawDataTypeConstants
	Protected mSourceFilePath As String = String.Empty
	Protected ReadOnly mOutputType As clsAnalysisResources.MSXMLOutputTypeConstants

	Protected ReadOnly mCentroidMS1 As Boolean
	Protected ReadOnly mCentroidMS2 As Boolean

	Protected mUseProgRunnerResultCode As Boolean		' When true, then return an error if the progrunner returns a non-zero exit code

	Protected mErrorMessage As String = String.Empty
	Protected mDebugLevel As Integer = 1

	Protected WithEvents CmdRunner As clsRunDosProgram

    Public Event ProgRunnerStarting(CommandLine As String)
    Public Event LoopWaiting()

#End Region

#Region "Properties"

    Public Property DebugLevel() As Integer
        Get
            Return mDebugLevel
        End Get
        Set(value As Integer)
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

    Public MustOverride ReadOnly Property ProgramName As String

    Public ReadOnly Property SourceFilePath As String
        Get
            Return mSourceFilePath
        End Get
    End Property

#End Region

    Public Sub New(
      WorkDir As String,
      ProgramPath As String,
      DatasetName As String,
      RawDataType As clsAnalysisResources.eRawDataTypeConstants,
      eOutputType As clsAnalysisResources.MSXMLOutputTypeConstants,
      CentroidMSXML As Boolean)

        mWorkDir = WorkDir
        mProgramPath = ProgramPath
        mDatasetName = DatasetName
        mRawDataType = RawDataType
        mOutputType = eOutputType
        mCentroidMS1 = CentroidMSXML
        mCentroidMS2 = CentroidMSXML

        mErrorMessage = String.Empty
    End Sub

    Public Sub New(
      WorkDir As String,
      ProgramPath As String,
      DatasetName As String,
      RawDataType As clsAnalysisResources.eRawDataTypeConstants,
      eOutputType As clsAnalysisResources.MSXMLOutputTypeConstants,
      CentroidMS1 As Boolean,
      CentroidMS2 As Boolean)

        mWorkDir = WorkDir
        mProgramPath = ProgramPath
        mDatasetName = DatasetName
        mRawDataType = RawDataType
        mOutputType = eOutputType
        mCentroidMS1 = CentroidMS1
        mCentroidMS2 = CentroidMS2

        mErrorMessage = String.Empty
    End Sub

    Protected MustOverride Function CreateArguments(msXmlFormat As String, RawFilePath As String) As String

    ''' <summary>
    ''' Generate the mzXML or mzML file
    ''' </summary>
    ''' <returns>True if success; false if a failure</returns>
    ''' <remarks></remarks>
    Public Function CreateMSXMLFile() As Boolean
        Dim msXmlFormat = "mzXML"

        Select Case mRawDataType
            Case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile
                mSourceFilePath = Path.Combine(mWorkDir, mDatasetName & clsAnalysisResources.DOT_RAW_EXTENSION)
            Case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder, clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf
                mSourceFilePath = Path.Combine(mWorkDir, mDatasetName & clsAnalysisResources.DOT_D_EXTENSION)
            Case Else
                Throw New ArgumentOutOfRangeException("Unsupported raw data type: " + mRawDataType.ToString())
        End Select


        Dim blnSuccess As Boolean

        mErrorMessage = String.Empty

        Select Case mOutputType
            Case clsAnalysisResources.MSXMLOutputTypeConstants.mzXML
                msXmlFormat = "mzXML"
            Case clsAnalysisResources.MSXMLOutputTypeConstants.mzML
                msXmlFormat = "mzML"
        End Select

        CmdRunner = New clsRunDosProgram(Path.GetDirectoryName(mProgramPath))

        ' Verify that program file exists
        If Not File.Exists(mProgramPath) Then
            mErrorMessage = "Cannot find MSXmlGenerator exe program file: " & mProgramPath
            Return False
        End If

        'Set up and execute a program runner to run MS XML executable

        Dim cmdStr = CreateArguments(msXmlFormat, mSourceFilePath)

        blnSuccess = SetupTool()
        If Not blnSuccess Then
            If String.IsNullOrEmpty(mErrorMessage) Then
                mErrorMessage = "SetupTool returned false"
            End If
            Return False
        End If

        RaiseEvent ProgRunnerStarting(mProgramPath & cmdStr)

        With CmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = True

            .EchoOutputToConsole = True

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = Path.Combine(mWorkDir, Path.GetFileNameWithoutExtension(mProgramPath) & "_ConsoleOutput.txt")

            .WorkDir = mWorkDir
        End With

        Dim dtStartTime = DateTime.UtcNow()
        blnSuccess = CmdRunner.RunProgram(mProgramPath, cmdStr, Path.GetFileNameWithoutExtension(mProgramPath), mUseProgRunnerResultCode, MAX_RUNTIME_SECONDS)

        If Not String.IsNullOrWhiteSpace(CmdRunner.CachedConsoleErrors) Then
            ' Append the console errors to the log file
            Using swConsoleOutputFile = New StreamWriter(New FileStream(CmdRunner.ConsoleOutputFilePath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
                swConsoleOutputFile.WriteLine()
                swConsoleOutputFile.WriteLine(CmdRunner.CachedConsoleErrors)
            End Using

            Dim consoleError = "Console error: " & CmdRunner.CachedConsoleErrors.Replace(Environment.NewLine, "; ")
            If String.IsNullOrWhiteSpace(mErrorMessage) Then
                mErrorMessage = consoleError
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, consoleError)
            End If
            blnSuccess = False
        End If

        If Not blnSuccess Then
            If DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds >= MAX_RUNTIME_SECONDS Then
                mErrorMessage = ProgramName & " has run for over " & DateTime.UtcNow.Subtract(dtStartTime).TotalHours.ToString("0") & " hours and has thus been aborted"
                Return False
            Else
                If CmdRunner.ExitCode <> 0 Then
                    mErrorMessage = Path.GetFileNameWithoutExtension(mProgramPath) & " returned a non-zero exit code: " & CmdRunner.ExitCode.ToString
                    Return False
                Else
                    mErrorMessage = "Call to " & Path.GetFileNameWithoutExtension(mProgramPath) & " failed (but exit code is 0)"
                    Return True
                End If
            End If
        Else
            ' Make sure the output file was created and is not empty
            Dim outputFilePath As String
            outputFilePath = Path.ChangeExtension(mSourceFilePath, msXmlFormat)

            If Not File.Exists(outputFilePath) Then
                mErrorMessage = "Output file not found: " & outputFilePath
                Return False
            End If

            ' Validate that the output file is complete
            If Not ValidateMsXmlFile(mOutputType, outputFilePath) Then
                Return False
            End If

            Return True

        End If

    End Function

    Public Sub LogCreationStatsRawToMzXml(dtStartTimeUTC As DateTime, strWorkDirPath As String, strDatasetName As String)

        Dim strSourceFilePath As String = Path.Combine(strWorkDirPath, strDatasetName & clsAnalysisResources.DOT_RAW_EXTENSION)
        Dim strMsXmlFilePath As String = Path.Combine(strWorkDirPath, strDatasetName & clsAnalysisResources.DOT_MZXML_EXTENSION)

        LogCreationStatsSourceToMsXml(dtStartTimeUTC, strSourceFilePath, strMsXmlFilePath)

    End Sub

    Public Sub LogCreationStatsSourceToMsXml(dtStartTimeUTC As DateTime, strSourceFilePath As String, strMsXmlFilePath As String)

        Try
            ' Save some stats to the log

            Dim strMessage As String
            Dim ioFileInfo As FileInfo
            Dim dblSourceFileSizeMB As Double, dblMsXmlSizeMB As Double
            Dim dblTotalMinutes As Double

            Dim strSourceFileExtension As String = Path.GetExtension(strSourceFilePath)
            Dim strTargetFileExtension As String = Path.GetExtension(strMsXmlFilePath)

            dblTotalMinutes = DateTime.UtcNow.Subtract(dtStartTimeUTC).TotalMinutes

            ioFileInfo = New FileInfo(strSourceFilePath)
            If ioFileInfo.Exists Then
                dblSourceFileSizeMB = ioFileInfo.Length / 1024.0 / 1024
            End If

            ioFileInfo = New FileInfo(strMsXmlFilePath)
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

    Protected Function ValidateMsXmlFile(eOutputType As clsAnalysisResources.MSXMLOutputTypeConstants, outputFilePath As String) As Boolean
        ' Open the .mzXML or .mzML file and look for </mzXML> or </indexedmzML> at the end of the file

        Try
            Dim mostRecentLine = String.Empty

            Dim fiOutputFile = New FileInfo(outputFilePath)

            If Not fiOutputFile.Exists Then
                mErrorMessage = "Output file not found: " & fiOutputFile.FullName
                Return False
            End If

            Using srMsXmlfile = New StreamReader(New FileStream(outputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                While Not srMsXmlfile.EndOfStream
                    Dim dataLine = srMsXmlfile.ReadLine()
                    If Not String.IsNullOrWhiteSpace(dataLine) Then
                        mostRecentLine = dataLine
                    End If
                End While
            End Using

            mostRecentLine = mostRecentLine.Trim()
            If mostRecentLine.Length > 250 Then
                mostRecentLine = mostRecentLine.Substring(0, 250)
            End If

            Select Case eOutputType
                Case clsAnalysisResources.MSXMLOutputTypeConstants.mzXML
                    If mostRecentLine <> "</mzXML>" Then
                        mErrorMessage = "File " & fiOutputFile.Name & " is corrupt; it does not end in </mzXML>"
                        If String.IsNullOrWhiteSpace(mostRecentLine) Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "mzXML file is corrupt; file is empty or only contains whitespace")
                        Else
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "mzXML file is corrupt; final line is: " & mostRecentLine)
                        End If
                        Return False
                    End If

                Case clsAnalysisResources.MSXMLOutputTypeConstants.mzML
                    If mostRecentLine <> "</indexedmzML>" Then
                        mErrorMessage = "File " & fiOutputFile.Name & " is corrupt; it does not end in </indexedmzML>"
                        If String.IsNullOrWhiteSpace(mostRecentLine) Then
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "mzML file is corrupt; file is empty or only contains whitespace")
                        Else
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "mzML file is corrupt; final line is: " & mostRecentLine)
                        End If
                        Return False
                    End If

                Case Else
                    mErrorMessage = "Unrecognized output type: " & eOutputType.ToString()
                    Return False

            End Select

            Return True

        Catch ex As Exception
            mErrorMessage = "Exception validating the .mzXML or .mzML file"
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, mErrorMessage, ex)
            Return False
        End Try

    End Function


    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
        RaiseEvent LoopWaiting()
    End Sub
End Class
