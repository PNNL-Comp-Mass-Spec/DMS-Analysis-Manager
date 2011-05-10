Option Strict On

Imports AnalysisManagerBase
Imports PRISM.Files
Imports PRISM.Files.clsFileTools

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
    Protected mOutputType As MSXMLOutputTypeConstants
    Protected mCentroidMSXML As Boolean

    Protected mErrorMessage As String = String.Empty

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


    Public Sub New(ByVal WorkDir As String, _
                   ByVal ProgramPath As String, _
                   ByVal DatasetName As String, _
                   ByVal eOutputType As MSXMLOutputTypeConstants, _
                   ByVal CentroidMSXML As Boolean)

        mWorkDir = WorkDir
        mProgramPath = ProgramPath
        mDatasetName = DatasetName
        mOutputType = eOutputType
        mCentroidMSXML = CentroidMSXML

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
        Dim RawFilePath As String = System.IO.Path.Combine(mWorkDir, mDatasetName & AnalysisManagerBase.clsAnalysisResources.DOT_RAW_EXTENSION)

        Dim blnSuccess As Boolean

        mErrorMessage = String.Empty

        Select Case mOutputType
            Case MSXMLOutputTypeConstants.mzXML
                msXmlFormat = "mzXML"
            Case MSXMLOutputTypeConstants.mzML
                msXmlFormat = "mzML"
        End Select

        CmdRunner = New clsRunDosProgram(System.IO.Path.GetDirectoryName(mProgramPath))

        ' Verify that program file exists
        If Not System.IO.File.Exists(mProgramPath) Then
            mErrorMessage = "Cannot find MSXmlGenerator exe program file: " & mProgramPath
            Return False
        End If

        'Set up and execute a program runner to run MS XML executable

        CmdStr = CreateArguments(msXmlFormat, RawFilePath)

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
            .ConsoleOutputFilePath = System.IO.Path.Combine(mWorkDir, System.IO.Path.GetFileNameWithoutExtension(mProgramPath) & "_ConsoleOutput.txt")
        End With

        blnSuccess = CmdRunner.RunProgram(mProgramPath, CmdStr, System.IO.Path.GetFileNameWithoutExtension(mProgramPath), True)

        If Not blnSuccess Then
            If CmdRunner.ExitCode <> 0 Then
                mErrorMessage = System.IO.Path.GetFileNameWithoutExtension(mProgramPath) & " returned a non-zero exit code: " & CmdRunner.ExitCode.ToString
                blnSuccess = False
            Else
                mErrorMessage = "Call to " & System.IO.Path.GetFileNameWithoutExtension(mProgramPath) & " failed (but exit code is 0)"
                blnSuccess = True
            End If
        End If

        Return blnSuccess

    End Function

    Protected MustOverride Function SetupTool() As Boolean

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
        RaiseEvent LoopWaiting()
    End Sub
End Class
