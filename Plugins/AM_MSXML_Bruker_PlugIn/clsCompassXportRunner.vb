'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 03/30/2011
'
' Uses CompassXport to create a .mzXML or .mzML file
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase

Public Class clsCompassXportRunner

#Region "Enums"
    Public Enum MSXMLOutputTypeConstants
        Invalid = -1
        mzXML = 0
        mzData = 1
        mzML = 2
        JCAMP = 3
        CMD = 4
    End Enum
#End Region

#Region "Module Variables"
    Private mWorkDir As String
    Private mCompassXportProgramPath As String
    Private mDatasetName As String
    Private mOutputType As MSXMLOutputTypeConstants
    Private mCentroidMSXML As Boolean

    Private mErrorMessage As String = String.Empty

    Protected WithEvents CmdRunner As clsRunDosProgram

    Public Event ProgRunnerStarting(ByVal CommandLine As String)
    Public Event LoopWaiting()

#End Region

#Region "Properties"
    Public ReadOnly Property ErrorMessage() As String
        Get
            If mErrorMessage Is Nothing Then
                Return String.Empty
            Else
                Return mErrorMessage
            End If
        End Get
    End Property
#End Region

#Region "Methods"
    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <remarks>Presently not used</remarks>
    Public Sub New(ByVal WorkDir As String, _
                   ByVal CompassXportProgramPath As String, _
                   ByVal DatasetName As String, _
                   ByVal eOutputType As MSXMLOutputTypeConstants, _
                   ByVal CentroidMSXML As Boolean)

        mWorkDir = WorkDir
        mCompassXportProgramPath = CompassXportProgramPath
        mDatasetName = DatasetName
        mOutputType = eOutputType
        mCentroidMSXML = CentroidMSXML

        mErrorMessage = String.Empty
    End Sub

    ''' <summary>
    ''' Generate the mzXML or mzML file
    ''' </summary>
    ''' <returns>True if success; false if a failure</returns>
    ''' <remarks></remarks>
    Public Function CreateMSXMLFile() As Boolean
        Dim CmdStr As String

        Dim strMSXmlFormatName As String = "mzXML"
        Dim intFormatMode As Integer

		Dim strSourceFolderPath As String
		Dim strInputFilePath As String
		Dim strOutputFilePath As String

		Dim blnSuccess As Boolean

		mErrorMessage = String.Empty

		' Resolve the output file format
		If mOutputType = MSXMLOutputTypeConstants.Invalid Then
			mOutputType = MSXMLOutputTypeConstants.mzXML
			intFormatMode = 0
		Else
			intFormatMode = CInt(mOutputType)
		End If

		strMSXmlFormatName = GetMsXmlOutputTypeByID(mOutputType)

		' Define the input file path
		strSourceFolderPath = System.IO.Path.Combine(mWorkDir, mDatasetName & clsAnalysisResources.DOT_D_EXTENSION)
		strInputFilePath = System.IO.Path.Combine(strSourceFolderPath, "analysis.baf")

		If Not System.IO.File.Exists(strInputFilePath) Then
			' Analysis.baf not found; look for analysis.yep instead
			strInputFilePath = System.IO.Path.Combine(strSourceFolderPath, "analysis.yep")

			If Not System.IO.File.Exists(strInputFilePath) Then
				mErrorMessage = "Could not find analysis.baf or analysis.yep in " & mDatasetName & clsAnalysisResources.DOT_D_EXTENSION
				Return False
			End If
		End If

		' Define the output file path
		strOutputFilePath = System.IO.Path.Combine(mWorkDir, mDatasetName & "." & strMSXmlFormatName)

		' Verify that program file exists
		If Not System.IO.File.Exists(mCompassXportProgramPath) Then
			mErrorMessage = "Cannot find CompassXport exe program file: " & mCompassXportProgramPath
			Return False
		End If

		CmdRunner = New clsRunDosProgram(System.IO.Path.GetDirectoryName(mCompassXportProgramPath))

		'Set up and execute a program runner to run CompassXport executable

		CmdStr = " -mode " & intFormatMode.ToString() & _
				 " -a " & strInputFilePath & _
				 " -o " & strOutputFilePath

        If mCentroidMSXML Then
            ' Centroiding is enabled
            CmdStr &= " -raw 0"
        Else
            CmdStr &= " -raw 1"
        End If

        RaiseEvent ProgRunnerStarting(mCompassXportProgramPath & CmdStr)

        With CmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = True
            .EchoOutputToConsole = True

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = System.IO.Path.Combine(mWorkDir, System.IO.Path.GetFileNameWithoutExtension(mCompassXportProgramPath) & "_ConsoleOutput.txt")
        End With

        blnSuccess = CmdRunner.RunProgram(mCompassXportProgramPath, CmdStr, System.IO.Path.GetFileNameWithoutExtension(mCompassXportProgramPath), True)

        If Not blnSuccess Then
            If CmdRunner.ExitCode <> 0 Then
                mErrorMessage = System.IO.Path.GetFileNameWithoutExtension(mCompassXportProgramPath) & " returned a non-zero exit code: " & CmdRunner.ExitCode.ToString
                blnSuccess = False
            Else
                mErrorMessage = "Call to " & System.IO.Path.GetFileNameWithoutExtension(mCompassXportProgramPath) & " failed (but exit code is 0)"
                blnSuccess = True
            End If
        End If

        Return blnSuccess

    End Function

    Public Shared Function GetMsXmlOutputTypeByID(ByVal eType As MSXMLOutputTypeConstants) As String
        Select Case eType
            Case MSXMLOutputTypeConstants.mzXML
                Return "mzXML"
            Case MSXMLOutputTypeConstants.mzData
                Return "mzData"
            Case MSXMLOutputTypeConstants.mzML
                Return "mzML"
            Case MSXMLOutputTypeConstants.JCAMP
                Return "JCAMP"
            Case MSXMLOutputTypeConstants.CMD
                Return "CMD"
            Case Else
                ' Includes MSXMLOutputTypeConstants.Invalid
                Return ""
        End Select
    End Function

    Public Shared Function GetMsXmlOutputTypeByName(ByVal strName As String) As MSXMLOutputTypeConstants
        Select Case strName.ToLower()
            Case "mzxml"
                Return MSXMLOutputTypeConstants.mzXML
            Case "mzdata"
                Return MSXMLOutputTypeConstants.mzData
            Case "mzml"
                Return MSXMLOutputTypeConstants.mzML
            Case "jcamp"
                Return MSXMLOutputTypeConstants.JCAMP
            Case "cmd"
                Return MSXMLOutputTypeConstants.CMD
            Case Else
                Return MSXMLOutputTypeConstants.Invalid
        End Select
    End Function

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
        RaiseEvent LoopWaiting()
    End Sub
#End Region

End Class
