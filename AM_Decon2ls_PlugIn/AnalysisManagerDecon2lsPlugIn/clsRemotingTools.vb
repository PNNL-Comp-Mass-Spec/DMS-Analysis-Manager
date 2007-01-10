'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 10/30/2006
'
' Last modified 10/30/2006
'*********************************************************************************************************

Imports System.IO
Imports PRISM.Logging
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
	'	Const SVR_CHAN As Integer = 54321
	Const SVR_FILE_NAME As String = "Decon2LSCAOServer.exe"
	Const FLAG_FILE_NAME As String = "FlagFile_Svr.txt"
#End Region

#Region "Module variables"
	Protected m_ToolObj As clsDecon2LSRemoter	 'Remote class for execution of Decon2LS via .Net remoting
	Protected m_Decon2LSRunner As clsProgRunner
	Protected m_Channel As TcpClientChannel
	Protected m_Logger As PRISM.Logging.ILogger
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
	Public Sub New(ByVal MyLogger As PRISM.Logging.ILogger, ByVal DebugLevel As Integer, ByVal TcpPort As Integer)

		m_Logger = MyLogger
		m_DebugLevel = DebugLevel
		m_TcpPort = TcpPort

	End Sub

	Public Function StartSvr() As Boolean

		'Starts the .Net Remoting CAO server

		If m_DebugLevel > 3 Then
			m_Logger.PostEntry("clsAnalysisToolRunnerDecon2LSBase.Setup(); initializing Remoting", ILogger.logMsgType.logDebug, True)
		End If

		'Initialize the remoting setup
		'Register a TCP client channel
		m_Channel = New TcpClientChannel

		ChannelServices.RegisterChannel(m_Channel)

		'Register the remote class as a valid type in the client's app domain by passing
		'the Remote class and its URL (it will be instantiated later)
		Dim MyTypeEntry As ActivatedClientTypeEntry = RemotingConfiguration.IsRemotelyActivatedClientType(GetType(clsDecon2LSRemoter))
		If MyTypeEntry Is Nothing Then
			RemotingConfiguration.RegisterActivatedClientType(GetType(clsDecon2LSRemoter), "tcp://localhost:" & m_TcpPort.ToString)
		End If

		'Start the remoting service via a ProgRunner
		Try
			m_Decon2LSRunner = New clsProgRunner
			With m_Decon2LSRunner
				.Arguments = "-o" & Path.GetDirectoryName(clsGlobal.AppFilePath) & " -p" & m_TcpPort.ToString
				.CreateNoWindow = False
				.MonitoringInterval = 100				 'Milliseconds
				.Program = Path.Combine(Path.GetDirectoryName(clsGlobal.AppFilePath), SVR_FILE_NAME)
				.RegisterEventLogger(m_Logger)
				.RegisterExceptionLogger(m_Logger)
				.Repeat = False
				.RepeatHoldOffTime = 0
				.WorkDir = Path.GetDirectoryName(clsGlobal.AppFilePath)
				.NotifyOnException = True
			End With
			m_Decon2LSRunner.StartAndMonitorProgram()
			If m_DebugLevel > 3 Then
				m_Logger.PostEntry("clsAnalysisToolRunnerDecon2LSBase.Setup(); Remoting server started", ILogger.logMsgType.logDebug, True)
			End If
			Return True
		Catch Ex As System.Exception
			m_Logger.PostEntry("clsAnalysisToolRunnerDecon2LSBase.Setup(); Remoting server startup error: " & Ex.Message, _
			 ILogger.logMsgType.logError, True)
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
			File.Delete(Path.Combine(Path.GetDirectoryName(clsGlobal.AppFilePath), FLAG_FILE_NAME))
		Catch ex As System.Exception
			m_Logger.PostEntry("clsAnalysisToolRunnerDecon2lsBase.RunTool(), Problem deleting remoting server flag file: " & ex.Message, _
			 ILogger.logMsgType.logError, True)
			Return False
		End Try

		System.Threading.Thread.Sleep(5000)		'Wait 5 seconds to ensure process has stopped
		m_Decon2LSRunner.StopMonitoringProgram(False)
		Return True

	End Function

#End Region

End Class
