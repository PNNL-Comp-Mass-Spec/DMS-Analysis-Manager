'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 07/12/2007
'
' Program converted from original version written by J.D. Sandoval, PNNL.
' Conversion performed as part of upgrade to VB.Net 2005, modification for use with manager and broker databases
'
'*********************************************************************************************************

Imports System.IO
Imports AnalysisManagerBase

''' <summary>
''' Calls the PeptideProphetRunner application
''' </summary>
''' <remarks></remarks>
Public Class clsPeptideProphetWrapper

#Region "Constants"
    Public Const MAX_PEPTIDE_PROPHET_RUNTIME_MINUTES As Integer = 120
#End Region

#Region "Module variables"
    Private ReadOnly m_PeptideProphetRunnerLocation As String = String.Empty
	Private m_ErrMsg As String = String.Empty
    Private m_DebugLevel As Short = 1
    Private m_InputFile As String = String.Empty

    Protected WithEvents CmdRunner As clsRunDosProgram

#End Region

    Public Event PeptideProphetRunning(PepProphetStatus As String, PercentComplete As Single)

#Region "Properties"
    Public Property DebugLevel() As Short
        Get
            Return m_DebugLevel
        End Get
        Set(value As Short)
            m_DebugLevel = value
        End Set
    End Property


    Public ReadOnly Property ErrMsg() As String
        Get
            If m_ErrMsg Is Nothing Then
                Return String.Empty
            Else
                Return m_ErrMsg
            End If
        End Get
    End Property

    Public Property InputFile() As String
        Get
            Return m_InputFile
        End Get
        Set(Value As String)
            m_InputFile = Value
        End Set
    End Property

    Public Property Enzyme As String = String.Empty

    Public Property OutputFolderPath As String = String.Empty

#End Region

#Region "Methods"

    Public Sub New(strPeptideProphetRunnerLocation As String)
        m_PeptideProphetRunnerLocation = strPeptideProphetRunnerLocation
    End Sub

    Public Function CallPeptideProphet() As IJobParams.CloseOutType

        Dim CmdStr As String
        Dim ioInputFile As FileInfo
        Dim strPeptideProphetConsoleOutputFilePath As String

        Try
            m_ErrMsg = String.Empty

            ioInputFile = New FileInfo(m_InputFile)
            strPeptideProphetConsoleOutputFilePath = Path.Combine(ioInputFile.DirectoryName, "PeptideProphetConsoleOutput.txt")

            CmdRunner = New clsRunDosProgram(ioInputFile.Directoryname)

            ' verify that program file exists
            If Not File.Exists(m_PeptideProphetRunnerLocation) Then
                m_ErrMsg = "PeptideProphetRunner not found at " & m_PeptideProphetRunnerLocation
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            ' Set up and execute a program runner to run the Peptide Prophet Runner
            CmdStr = ioInputFile.FullName & " " & ioInputFile.DirectoryName & " /T:" & MAX_PEPTIDE_PROPHET_RUNTIME_MINUTES
            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_PeptideProphetRunnerLocation & " " & CmdStr)
            End If

            With CmdRunner
                .CreateNoWindow = True
                .CacheStandardOutput = True
                .EchoOutputToConsole = True

                .WriteConsoleOutputToFile = True
                .ConsoleOutputFilePath = strPeptideProphetConsoleOutputFilePath
            End With

            If Not CmdRunner.RunProgram(m_PeptideProphetRunnerLocation, CmdStr, "PeptideProphetRunner", True) Then
                m_ErrMsg = "Error running PeptideProphetRunner"
                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            If CmdRunner.ExitCode <> 0 Then
                m_ErrMsg = "Peptide prophet runner returned a non-zero error code: " & CmdRunner.ExitCode.ToString

                ' Parse the console output file for any lines that contain "Error"
                ' Append them to m_ErrMsg

                Dim ioConsoleOutputFile As FileInfo = New FileInfo(strPeptideProphetConsoleOutputFilePath)
                Dim blnErrorMessageFound As Boolean = False

                If ioConsoleOutputFile.Exists Then
                    Dim srInFile As StreamReader
                    srInFile = New StreamReader(New FileStream(ioConsoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

                    Do While Not srInFile.EndOfStream
                        Dim strLineIn As String
                        strLineIn = srInFile.ReadLine()
                        If Not String.IsNullOrWhiteSpace(strLineIn) Then
                            If strLineIn.ToLower.Contains("error") Then
                                m_ErrMsg &= "; " & m_ErrMsg
                                blnErrorMessageFound = True
                            End If
                        End If
                    Loop
                    srInFile.close()
                End If

                If Not blnErrorMessageFound Then
                    m_ErrMsg &= "; Unknown error message"
                End If

                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            RaiseEvent PeptideProphetRunning("Complete", 100)

            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

        Catch ex As Exception
            m_ErrMsg = "Exception while running peptide prophet: " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try


    End Function

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
        Static dtLastStatusUpdate As DateTime = DateTime.UtcNow

        'Update the status (limit the updates to every 5 seconds)
        If DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = DateTime.UtcNow
            RaiseEvent PeptideProphetRunning("Running", 50)
        End If

    End Sub

#End Region

End Class
