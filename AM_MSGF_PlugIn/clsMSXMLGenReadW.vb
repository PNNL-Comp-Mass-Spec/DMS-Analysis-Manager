'*********************************************************************************************************
' Written by Matthew Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Created 07/19/2010
'
' Uses ReadW to create a .mzXML or .mzML file
'*********************************************************************************************************

Option Strict On

Imports AnalysisManagerBase
Imports PRISM.Files
Imports PRISM.Files.clsFileTools

Public Class clsMSXMLGenReadW

#Region "Enums"
    Public Enum MSXMLOutputTypeConstants
        mzXML = 0
        mzML = 1
    End Enum
#End Region

#Region "Module Variables"
    Private mWorkDir As String
    Private mReadWProgramPath As String
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
                   ByVal ReadWProgramPath As String, _
                   ByVal DatasetName As String, _
                   ByVal eOutputType As MSXMLOutputTypeConstants, _
                   ByVal CentroidMSXML As Boolean)

        mWorkDir = WorkDir
        mReadWProgramPath = ReadWProgramPath
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

        Dim msXmlFormat As String = "mzXML"
        Dim RawFilePath As String = System.IO.Path.Combine(mWorkDir, mDatasetName & AnalysisManagerBase.clsAnalysisResources.DOT_RAW_EXTENSION)

        Dim blnSuccess As Boolean

        mErrorMessage = String.Empty

        Select Case mOutputType
            Case MSXMLOutputTypeConstants.mzXML
                msXmlFormat = "mzXML"
            Case MSXMLOutputTypeConstants.mzML
                msXmlFormat = "mzML"
        End Select

        CmdRunner = New clsRunDosProgram(System.IO.Path.GetDirectoryName(mReadWProgramPath))

        ' Verify that program file exists
        If Not System.IO.File.Exists(mReadWProgramPath) Then
            mErrorMessage = "Cannot find MSXmlGenerator exe program file: " & mReadWProgramPath
            Return False
        End If

        'Set up and execute a program runner to run MS XML executable
        If mCentroidMSXML Then
            ' Centroiding is enabled
            CmdStr = " --" & msXmlFormat & " " & " -c " & RawFilePath
        Else
            CmdStr = " --" & msXmlFormat & " " & RawFilePath
        End If

        RaiseEvent ProgRunnerStarting(mReadWProgramPath & CmdStr)

        With CmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = True
            .EchoOutputToConsole = True

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = System.IO.Path.Combine(mWorkDir, System.IO.Path.GetFileNameWithoutExtension(mReadWProgramPath) & "_ConsoleOutput.txt")
        End With

        blnSuccess = CmdRunner.RunProgram(mReadWProgramPath, CmdStr, System.IO.Path.GetFileNameWithoutExtension(mReadWProgramPath), True)

        If Not blnSuccess Then
            If CmdRunner.ExitCode <> 0 Then
                mErrorMessage = System.IO.Path.GetFileNameWithoutExtension(mReadWProgramPath) & " returned a non-zero exit code: " & CmdRunner.ExitCode.ToString
                blnSuccess = False
            Else
                mErrorMessage = "Call to " & System.IO.Path.GetFileNameWithoutExtension(mReadWProgramPath) & " failed (but exit code is 0)"
                blnSuccess = True
            End If
        End If

        Return blnSuccess

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
