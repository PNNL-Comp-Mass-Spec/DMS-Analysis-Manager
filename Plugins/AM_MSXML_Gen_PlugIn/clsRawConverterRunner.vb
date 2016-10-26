Imports System.IO
Imports AnalysisManagerBase

Public Class clsRawConverterRunner
    Inherits clsEventNotifier

#Region "Constants"
    Public Const RAWCONVERTER_FILENAME As String = "RawConverter.exe"
#End Region

#Region "Member variables"
    ''' <summary>
    ''' 0 means no debugging, 1 for normal, 2 for verbose
    ''' </summary>
    Private ReadOnly m_DebugLevel As Integer

#End Region

#Region "Properties"

    Public ReadOnly Property RawConverterExePath As String

#End Region

    ''' <summary>
    ''' Constructor
    ''' </summary>
    Public Sub New(rawConverterDir As String, Optional debugLevel As Integer = 1)

        RawConverterExePath = Path.Combine(rawConverterDir, RAWCONVERTER_FILENAME)
        If Not File.Exists(RawConverterExePath) Then
            Throw New FileNotFoundException(RawConverterExePath)
        End If

        m_DebugLevel = debugLevel

    End Sub

    ''' <summary>
    ''' Create .mgf file using RawConverter
    ''' This function is called by MakeDTAFilesThreaded
    ''' </summary>
    ''' <returns>TRUE for success; FALSE for failure</returns>
    ''' <remarks></remarks>
    Public Function ConvertRawToMGF(rawFilePath As String) As Boolean

        Try

            If m_DebugLevel > 0 Then
                OnProgressUpdate("Creating .MGF file using RawConverter", 0)
            End If

            Dim fiRawConverter = New FileInfo(RawConverterExePath)

            ' Set up command
            Dim cmdStr = " " & rawFilePath & " --mgf"

            If m_DebugLevel > 0 Then
                OnProgressUpdate(fiRawConverter.FullName & " " & cmdStr, 0)
            End If

            ' Setup a program runner tool to make the spectra files
            ' The working directory must be the folder that has RawConverter.exe
            ' Otherwise, the program creates the .mgf file in C:\  (and will likely get Access Denied)

            Dim progRunner = New clsRunDosProgram(fiRawConverter.Directory.FullName) With {
                .CreateNoWindow = True,
                .CacheStandardOutput = True,
                .EchoOutputToConsole = True,
                .WriteConsoleOutputToFile = True,
                .ConsoleOutputFilePath = String.Empty      ' Allow the console output filename to be auto-generated
            }

            AddHandler progRunner.ConsoleErrorEvent, AddressOf ProgRunner_ConsoleErrorEvent

            If Not progRunner.RunProgram(fiRawConverter.FullName, cmdStr, "RawConverter", True) Then
                ' .RunProgram returned False
                OnErrorEvent("Error running " & Path.GetFileNameWithoutExtension(fiRawConverter.Name))
                Return False
            End If

            If m_DebugLevel >= 2 Then
                OnProgressUpdate(" ... MGF file created using RawConverter", 100)
            End If

            Return True

        Catch ex As Exception
            OnErrorEvent("Exception in ConvertRawToMGF: " + ex.Message)
            Return False
        End Try

    End Function

    Private Sub ProgRunner_ConsoleErrorEvent(errMsg As String)
        OnErrorEvent("Exception running RawConverter: " + errMsg)
    End Sub
End Class
