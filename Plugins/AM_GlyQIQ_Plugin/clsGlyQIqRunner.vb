Imports AnalysisManagerBase
Imports System.IO
Imports PRISM.Processes
Imports System.Text.RegularExpressions

Public Class clsGlyQIqRunner

    Protected Const GLYQ_IQ_CONSOLE_OUTPUT_PREFIX As String = "GlyQ-IQ_ConsoleOutput_Core"

#Region "Enums"

    Public Enum GlyQIqRunnerStatusCodes
        NotStarted = 0
        Running = 1
        Success = 2
        Failure = 3
    End Enum

#End Region

#Region "Events"
    Public Event CmdRunnerWaiting()
#End Region

#Region "Properties"
    
    Public ReadOnly Property BatchFilePath As String
        Get
            Return mBatchFilePath
        End Get
    End Property

    Public ReadOnly Property ConsoleOutputFilePath As String
        Get
            Return mConsoleOutputFilePath
        End Get
    End Property

    Public ReadOnly Property Core As Integer
        Get
            Return mCore
        End Get
    End Property

    ''' <summary>
    ''' Value between 0 and 100
    ''' </summary>
    ''' <remarks></remarks>
    Public ReadOnly Property Progress As Integer
        Get
            Return mProgress
        End Get
    End Property


    Public ReadOnly Property ProgRunner As clsRunDosProgram
        Get
            Return mCmdRunner
        End Get
    End Property

    Public ReadOnly Property Status As GlyQIqRunnerStatusCodes
        Get
            Return mStatus
        End Get
    End Property

    Public ReadOnly Property ProgRunnerStatus As clsProgRunner.States
        Get
            If mCmdRunner Is Nothing Then
                Return clsProgRunner.States.NotMonitoring
            End If
            Return mCmdRunner.State
        End Get
    End Property

#End Region

#Region "Member Variables"

    Protected mBatchFilePath As String
    Protected mConsoleOutputFilePath As String
    Protected mCore As Integer
    Protected mProgress As Integer

    Protected mStatus As GlyQIqRunnerStatusCodes

    Protected ReadOnly mWorkingDirectory As String

    Protected WithEvents mCmdRunner As clsRunDosProgram

#End Region


    Public Sub New(ByVal workingDirectory As String, ByVal processingCore As Integer, batchFilePathToUse As String)
        mWorkingDirectory = workingDirectory
        mCore = processingCore
        mBatchFilePath = batchFilePathToUse
        mStatus = GlyQIqRunnerStatusCodes.NotStarted
    End Sub

    Public Sub StartAnalysis()

        mCmdRunner = New clsRunDosProgram(mWorkingDirectory)
        mProgress = 0

        mConsoleOutputFilePath = Path.Combine(mWorkingDirectory, GLYQ_IQ_CONSOLE_OUTPUT_PREFIX & mCore & ".txt")

        With mCmdRunner
            .CreateNoWindow = True
            .CacheStandardOutput = False
            .EchoOutputToConsole = False

            .WriteConsoleOutputToFile = True
            .ConsoleOutputFilePath = mConsoleOutputFilePath
        End With

        mStatus = GlyQIqRunnerStatusCodes.Running

        Dim cmdStr As String = String.Empty
        Dim blnSuccess = mCmdRunner.RunProgram(BatchFilePath, cmdStr, "GlyQ-IQ", True)

        If blnSuccess Then
            mStatus = GlyQIqRunnerStatusCodes.Success
        Else
            mStatus = GlyQIqRunnerStatusCodes.Failure
        End If

    End Sub


    ''' <summary>
    ''' Parse the GlyQ-IQ console output file to track the search progress
    ''' </summary>
    ''' <param name="strConsoleOutputFilePath"></param>
    ''' <remarks></remarks>
    Private Sub ParseConsoleOutputFile(ByVal strConsoleOutputFilePath As String)

        ' Example Console output, looking for lines like this
        '

        Static reProgressStats As Regex = New Regex("We found (?<Done>\d+) out of (?<Total>\d+)", RegexOptions.Compiled Or RegexOptions.IgnoreCase)
        Static dtLastProgressWriteTime As DateTime = DateTime.UtcNow

        Try
            If Not File.Exists(strConsoleOutputFilePath) Then

                Exit Sub
            End If

            Dim strLineIn As String
            Dim glyqIqProgress As Integer = 0

            Using srInFile = New StreamReader(New FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                Do While srInFile.Peek() >= 0
                    strLineIn = srInFile.ReadLine()

                    If Not String.IsNullOrWhiteSpace(strLineIn) Then

                        Dim reMatch = reProgressStats.Match(strLineIn)

                        If reMatch.Success Then
                            Dim intDone As Integer
                            Dim intTotal As Integer

                            If Integer.TryParse(reMatch.Groups("Done").Value, intDone) Then
                                If Integer.TryParse(reMatch.Groups("Total").Value, intTotal) Then
                                    glyqIqProgress = CInt(intDone / CSng(intTotal) * 100)
                                End If
                            End If
                        End If

                    End If
                Loop

            End Using

            If glyqIqProgress > mProgress Then
                mProgress = glyqIqProgress
            End If

        Catch ex As Exception
            ' Ignore errors here
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing console output file (" & strConsoleOutputFilePath & "): " & ex.Message)
        End Try

    End Sub


    Private Sub CmdRunner_LoopWaiting() Handles mCmdRunner.LoopWaiting

        Static dtLastConsoleOutputParse As DateTime = DateTime.UtcNow

        If DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15 Then
            dtLastConsoleOutputParse = DateTime.UtcNow

            ParseConsoleOutputFile(mConsoleOutputFilePath)

        End If

        RaiseEvent CmdRunnerWaiting()
    End Sub
End Class
