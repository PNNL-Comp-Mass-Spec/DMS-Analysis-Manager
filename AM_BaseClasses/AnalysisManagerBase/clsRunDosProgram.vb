Imports PRISM.Logging

Public Class clsRunDosProgram

	'Provides a looping wrapper around a ProgRunner object for running command-line programs

#Region "Module variables"
	Private m_CreateNoWindow As Boolean = True
	Private m_MonitorInterval As Integer = 2000	 'msec
	Private m_Logger As ILogger
	Private m_WorkDir As String
	Private m_DebugLevel As Integer = 0
#End Region

#Region "Events"
	Public Event LoopWaiting()	 'Class is waiting until next time it's due to check status of called program (good time for external processing)
#End Region
#Region "Properties"

	Public Property DebugLevel() As Integer
		Get
			Return m_DebugLevel
		End Get
		Set(ByVal Value As Integer)
			m_DebugLevel = Value
		End Set
	End Property

	Public Property CreateNoWindow() As Boolean
		Get
			Return m_CreateNoWindow
		End Get
		Set(ByVal Value As Boolean)
			m_CreateNoWindow = Value
		End Set
	End Property

	Public Property MonitorInterval() As Integer	 'msec
		Get
			Return m_MonitorInterval
		End Get
		Set(ByVal Value As Integer)
			m_MonitorInterval = Value
		End Set
	End Property

	Public Property WorkDir() As String
		Get
			Return m_WorkDir
		End Get
		Set(ByVal Value As String)
			m_WorkDir = Value
		End Set
	End Property
#End Region

	Sub New(ByVal Logger As ILogger, ByVal WorkDir As String)

		m_Logger = Logger
		m_WorkDir = WorkDir

	End Sub

	Public Function RunProgram(ByVal ProgNameLoc As String, ByVal CmdLine As String, ByVal ProgName As String, _
	 Optional ByVal UseResCode As Boolean = False) As Boolean

		'Runs specified program
		Dim ProgRunner As New PRISM.Processes.clsProgRunner

		'DAC debugging
		'		System.Threading.Thread.CurrentThread.Name = "RunProg"

		With ProgRunner
			.Arguments = CmdLine
			.CreateNoWindow = m_CreateNoWindow
			.MonitoringInterval = m_MonitorInterval
			.Name = ProgName
			.Program = ProgNameLoc
			.Repeat = False
			.RepeatHoldOffTime = 0
			.WorkDir = m_WorkDir
		End With

		If m_DebugLevel > 3 Then
			m_Logger.PostEntry("clsAnalysisToolRunnerSeqBase.RunProgram(), ProgRunner.Arguments = " & ProgRunner.Arguments, _
			 ILogger.logMsgType.logDebug, True)
			m_Logger.PostEntry("clsAnalysisToolRunnerSeqBase.RunProgram(), ProgRunner.Program = " & ProgRunner.Program, _
			 ILogger.logMsgType.logDebug, True)
		End If

		'DAC debugging
		Debug.WriteLine("clsRunDOSProg.RunProgram, starting program, thread " & System.Threading.Thread.CurrentThread.Name)
		Try
			'Start the program executing
			ProgRunner.StartAndMonitorProgram()
			'loop until program is complete
			While (ProgRunner.State <> 0) And (ProgRunner.State <> 10)
				RaiseEvent LoopWaiting()
				System.Threading.Thread.Sleep(m_MonitorInterval)
			End While
		Catch ex As Exception
			m_Logger.PostError("Exception running DOS program " & ProgNameLoc, ex, True)
			Return False
		End Try


		If ProgRunner.State = 10 Or (UseResCode And ProgRunner.ExitCode <> 0) Then
			Return False
		Else
			Return True
		End If

		'DAC debugging
		Debug.WriteLine("clsRunDOSProg.RunProgram, program complete, thread " & System.Threading.Thread.CurrentThread.Name)
		Debug.WriteLine("Thread priority: " & System.Threading.Thread.CurrentThread.Priority)

		ProgRunner = Nothing

	End Function

End Class
