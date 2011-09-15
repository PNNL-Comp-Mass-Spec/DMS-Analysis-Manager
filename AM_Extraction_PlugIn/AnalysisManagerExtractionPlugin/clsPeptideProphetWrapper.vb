'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 07/12/2007
'
' Program converted from original version written by J.D. Sandoval, PNNL.
' Conversion performed as part of upgrade to VB.Net 2005, modification for use with manager and broker databases
'
' Last modified 07/12/2007
' Modified for mini-pipeline by DAC - 09/24/2008
' Last modified 06/15/2009 JDS - Added logging using log4net
' Updated 7/7/2011 by MEM to use the PeptideProphetRunner console app
'*********************************************************************************************************

Imports AnalysisManagerBase

Public Class clsPeptideProphetWrapper

	'*********************************************************************************************************
    ' Calls the PeptideProphetRunner application
	'*********************************************************************************************************

#Region "Constants"
    Public Const MAX_PEPTIDE_PROPHET_RUNTIME_MINUTES As Integer = 120
#End Region

#Region "Module variables"
    Private m_PeptideProphetRunnerLocation As String = ""
    Private m_ErrMsg As String = ""
    Private m_DebugLevel As Short = 1
    Private m_InputFile As String = ""
    Private m_OutputFolderPath As String = ""
    Private m_Enzyme As String = ""

    Protected WithEvents CmdRunner As clsRunDosProgram

#End Region

    Public Event PeptideProphetRunning(ByVal PepProphetStatus As String, ByVal PercentComplete As Single)

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
        Set(ByVal Value As String)
            m_InputFile = Value
        End Set
    End Property

    Public Property Enzyme() As String
        Get
            Return m_Enzyme
        End Get
        Set(ByVal Value As String)
            m_Enzyme = Value
        End Set
    End Property

    Public Property OutputFolderPath() As String
        Get
            Return m_OutputFolderPath
        End Get
        Set(ByVal Value As String)
            m_OutputFolderPath = Value
        End Set
    End Property
#End Region

#Region "Methods"

    Public Sub New(ByVal strPeptideProphetRunnerLocation As String)
        m_PeptideProphetRunnerLocation = strPeptideProphetRunnerLocation
    End Sub

    Public Function CallPeptideProphet() As IJobParams.CloseOutType

        Dim CmdStr As String
        Dim ioInputFile As System.IO.FileInfo
        Dim strPeptideProphetConsoleOutputFilePath As String

        Try
            m_ErrMsg = String.Empty

            ioInputFile = New System.IO.FileInfo(m_InputFile)
            strPeptideProphetConsoleOutputFilePath = System.IO.Path.Combine(ioInputFile.DirectoryName, "PeptideProphetConsoleOutput.txt")

            CmdRunner = New clsRunDosProgram(ioInputFile.Directoryname)

            ' verify that program file exists
            If Not System.IO.File.Exists(m_PeptideProphetRunnerLocation) Then
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

                Dim ioConsoleOutputFile As System.IO.FileInfo = New System.IO.FileInfo(strPeptideProphetConsoleOutputFilePath)
                Dim blnErrorMessageFound As Boolean = False

                If ioConsoleOutputFile.Exists Then
                    Dim srInFile As System.IO.StreamReader
                    srInFile = New System.IO.StreamReader(New System.IO.FileStream(ioConsoleOutputFile.FullName, IO.FileMode.Open, IO.FileAccess.Read, IO.FileShare.ReadWrite))

                    Do While srInFile.Peek() >= 0
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

        Catch ex As System.Exception
            m_ErrMsg = "Exception while running peptide prophet: " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try


    End Function

    ''' <summary>
    ''' Event handler for CmdRunner.LoopWaiting event
    ''' </summary>
    ''' <remarks></remarks>
    Private Sub CmdRunner_LoopWaiting() Handles CmdRunner.LoopWaiting
        Static dtLastStatusUpdate As System.DateTime = System.DateTime.UtcNow

        'Update the status (limit the updates to every 5 seconds)
        If System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5 Then
            dtLastStatusUpdate = System.DateTime.UtcNow
            RaiseEvent PeptideProphetRunning("Running", 50)
        End If

    End Sub

#End Region

End Class
