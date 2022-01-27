'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 10/30/2006
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Imports System.IO
Imports PRISM.Processes
Imports AnalysisManagerBase
Imports System.Runtime.Remoting
Imports System.Runtime.Remoting.Channels
Imports System.Runtime.Remoting.Channels.tcp
Imports Decon2LSRemoter

Public Class clsRemotingTools

    '*********************************************************************************************************
    'Class to provide tools for operations performed by .Net Remoting. Primary tools are remoting
    'server startup/shutdown.
    '*********************************************************************************************************

#Region "Constants"
    '    Const SVR_CHAN As Integer = 54321
    Const SVR_FILE_NAME As String = "Decon2LSCAOServer.exe"
    Const FLAG_FILE_NAME As String = "FlagFile_Svr.txt"
#End Region

#Region "Module variables"
    Protected m_ToolObj As clsDecon2LSRemoter     'Remote class for execution of Decon2LS via .Net remoting
    Protected m_Decon2LSRunner As ProgRunner
    Protected m_Channel As TcpClientChannel
    Protected m_DebugLevel As Integer
    Protected m_ErrMsg As String = ""
    Protected m_TcpPort As Integer = 0
#End Region

#Region "Properties"
    Public ReadOnly Property ErrMsg() As String
        Get
            Return m_ErrMsg
        End Get
    End Property
#End Region

#Region "Methods"
    Public Sub New(ByVal DebugLevel As Integer, ByVal TcpPort As Integer)

        m_DebugLevel = DebugLevel
        m_TcpPort = TcpPort

    End Sub

    Public Function StartSvr() As Boolean

        Dim strOutputFolderPath As String

        'Starts the .Net Remoting CAO server

        If m_DebugLevel > 3 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2LSBase.Setup(); initializing Remoting")
        End If

        'Initialize the remoting setup
        'Register a TCP client channel
        m_Channel = New TcpClientChannel

        ChannelServices.RegisterChannel(m_Channel, False)

        'Register the remote class as a valid type in the client's app domain by passing
        'the Remote class and its URL (it will be instantiated later)
        Dim MyTypeEntry As ActivatedClientTypeEntry = RemotingConfiguration.IsRemotelyActivatedClientType(GetType(clsDecon2LSRemoter))
        If MyTypeEntry Is Nothing Then
            RemotingConfiguration.RegisterActivatedClientType(GetType(clsDecon2LSRemoter), "tcp://localhost:" & m_TcpPort.ToString)
        End If

        'Start the remoting service via a ProgRunner
        Try
            m_Decon2LSRunner = New ProgRunner
            strOutputFolderPath = clsGlobal.GetAppFolderPath()
            If strOutputFolderPath.IndexOf(" ") >= 0 Then
                strOutputFolderPath = """" & strOutputFolderPath & """"
            End If

            With m_Decon2LSRunner
                .Arguments = "-o" & strOutputFolderPath & " -p" & m_TcpPort.ToString
                .CreateNoWindow = False
                .MonitoringInterval = 100                'Milliseconds
                .Program = Path.Combine(clsGlobal.GetAppFolderPath(), SVR_FILE_NAME)
                '                .RegisterEventLogger(m_Logger)
                'Research                .RegisterExceptionLogger()
                .Repeat = False
                .RepeatHoldOffTime = 0
                .WorkDir = clsGlobal.GetAppFolderPath()
                .NotifyOnException = True
            End With
            m_Decon2LSRunner.StartAndMonitorProgram()
            If m_DebugLevel > 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerDecon2LSBase.Setup(); Remoting server started")
            End If
            Return True
        Catch Ex As System.Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisToolRunnerDecon2LSBase.Setup(); Remoting server startup error: " & Ex.Message & "; " & clsGlobal.GetExceptionStackTrace(Ex))
            Return False
        End Try

    End Function

    Public Function StopSvr() As Boolean

        'Stops the .Net remoting server

        'Unregister the TCP channel that was used for communication with the remoting server
        ChannelServices.UnregisterChannel(m_Channel)

        'Stop the remoting server process, which releases file lock on raw data file
        'Stopping server is accomplished by deleting the flag file that was created by the server process
        Try
            File.Delete(Path.Combine(clsGlobal.GetAppFolderPath(), FLAG_FILE_NAME))
        Catch ex As System.Exception
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsRemotingTools.StopSvr(), Problem deleting remoting server flag file: " & ex.Message)
            Return False
        End Try

        System.Threading.Thread.Sleep(5000)        'Wait 5 seconds to ensure process has stopped
        m_Decon2LSRunner.StopMonitoringProgram(False)
        Return True

    End Function

#End Region

End Class
