'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Option Strict On

Imports System.Threading

Public Class clsRunDosProgram

	'*********************************************************************************************************
	'Provides a looping wrapper around a ProgRunner object for running command-line programs
	'*********************************************************************************************************

#Region "Module variables"
	Private m_CreateNoWindow As Boolean = True
	Private m_MonitorInterval As Integer = 2000	 'msec
	Private m_MaxRuntimeSeconds As Integer = 0

	Private m_WorkDir As String
	Private m_DebugLevel As Integer = 0
	Private m_ExitCode As Integer = 0

	Private m_CacheStandardOutput As Boolean = False
	Private m_EchoOutputToConsole As Boolean = True

    Private m_CachedConsoleErrors As String = String.Empty

	Private m_WriteConsoleOutputToFile As Boolean = False
	Private m_ConsoleOutputFilePath As String = String.Empty

	Private m_AbortProgramNow As Boolean
	Private m_AbortProgramPostLogEntry As Boolean

	'Runs specified program
	Private WithEvents m_ProgRunner As PRISM.Processes.clsProgRunner

#End Region

#Region "Events"
	''' <summary>
	''' Class is waiting until next time it's due to check status of called program (good time for external processing)
	''' </summary>
	''' <remarks></remarks>
	Public Event LoopWaiting()

	''' <summary>
	''' Text was written to the console
	''' </summary>
	''' <param name="NewText"></param>
	''' <remarks></remarks>
	Public Event ConsoleOutputEvent(ByVal NewText As String)

	''' <summary>
	''' Error message was written to the console
	''' </summary>
	''' <param name="NewText"></param>
	''' <remarks></remarks>
	Public Event ConsoleErrorEvent(ByVal NewText As String)

	''' <summary>
	''' Program execution exceeded MaxRuntimeSeconds
	''' </summary>
	''' <remarks></remarks>
	Public Event Timeout()

#End Region

#Region "Properties"

    ''' <summary>
    ''' Text written to the Error stream by the external program (including carriage returns)
    ''' </summary>
    Public ReadOnly Property CachedConsoleErrors() As String
        Get
            If String.IsNullOrWhiteSpace(m_CachedConsoleErrors) Then
                Return String.Empty
            Else
                Return m_CachedConsoleErrors
            End If
        End Get
    End Property

	''' <summary>
	''' Text written to the Console by the external program (including carriage returns)
	''' </summary>
	Public ReadOnly Property CachedConsoleOutput() As String
		Get
			If m_ProgRunner Is Nothing Then
				Return String.Empty
			Else
				Return m_ProgRunner.CachedConsoleOutput
			End If
		End Get
	End Property

	''' <summary>
	''' Any text written to the Error buffer by the external program
	''' </summary>
	Public ReadOnly Property CachedConsoleError() As String
		Get
			If m_ProgRunner Is Nothing Then
				Return String.Empty
			Else
				Return m_ProgRunner.CachedConsoleError
			End If
		End Get
	End Property

	''' <summary>
	''' When true then will cache the text the external program writes to the console
	''' Can retrieve using the CachedConsoleOutput readonly property
	''' Will also fire event ConsoleOutputEvent as new text is written to the console
	''' </summary>
	''' <remarks>If this is true, then no window will be shown, even if CreateNoWindow=False</remarks>
	Public Property CacheStandardOutput() As Boolean
		Get
			Return m_CacheStandardOutput
		End Get
		Set(ByVal value As Boolean)
			m_CacheStandardOutput = value
		End Set
	End Property

	''' <summary>
	''' File path to which the console output will be written if WriteConsoleOutputToFile is true
	''' If blank, then file path will be auto-defined in the WorkDir  when program execution starts
	''' </summary>
	''' <value></value>
	''' <returns></returns>
	''' <remarks></remarks>
	Public Property ConsoleOutputFilePath() As String
		Get
			Return m_ConsoleOutputFilePath
		End Get
		Set(ByVal value As String)
			If value Is Nothing Then value = String.Empty
			m_ConsoleOutputFilePath = value
		End Set
	End Property

	''' <summary>
	''' Determine if window should be displayed.
	''' Will be forced to True if CacheStandardOutput = True
	''' </summary>
	Public Property CreateNoWindow() As Boolean
		Get
			Return m_CreateNoWindow
		End Get
		Set(ByVal Value As Boolean)
			m_CreateNoWindow = Value
		End Set
	End Property

	''' <summary>
	''' Debug level for logging
	''' </summary>
	Public Property DebugLevel() As Integer
		Get
			Return m_DebugLevel
		End Get
		Set(ByVal Value As Integer)
			m_DebugLevel = Value
		End Set
	End Property

	''' <summary>
	''' When true, then echoes, in real time, text written to the Console by the external program 
	''' Ignored if CreateNoWindow = False
	''' </summary>
	Public Property EchoOutputToConsole() As Boolean
		Get
			Return m_EchoOutputToConsole
		End Get
		Set(ByVal value As Boolean)
			m_EchoOutputToConsole = value
		End Set
	End Property

	''' <summary>
	''' Exit code when process completes.
	''' </summary>
	Public ReadOnly Property ExitCode() As Integer
		Get
			Return m_ExitCode
		End Get
	End Property

	''' <summary>
	''' Maximum amount of time (seconds) that the program will be allowed to run; 0 if allowed to run indefinitely
	''' </summary>
	''' <value></value>
	Public ReadOnly Property MaxRuntimeSeconds As Integer
		Get
			Return m_MaxRuntimeSeconds
		End Get
	End Property

	''' <summary>
	''' How often (milliseconds) internal monitoring thread checks status of external program
	''' Minimum allowed value is 250 milliseconds
	''' </summary>
	Public Property MonitorInterval() As Integer	 'msec
		Get
			Return m_MonitorInterval
		End Get
		Set(ByVal Value As Integer)
			If Value < 250 Then Value = 250
			m_MonitorInterval = Value
		End Set
	End Property

	''' <summary>
	''' Returns true if program was aborted via call to AbortProgramNow()
	''' </summary>
	Public ReadOnly Property ProgramAborted() As Boolean
		Get
			Return m_AbortProgramNow
		End Get
	End Property

	''' <summary>
	''' Current monitoring state
	''' </summary>
	Public ReadOnly Property State() As PRISM.Processes.clsProgRunner.States
		Get
			If m_ProgRunner Is Nothing Then
				Return PRISM.Processes.clsProgRunner.States.NotMonitoring
			Else
				Return m_ProgRunner.State
			End If
		End Get
	End Property

	''' <summary>
	''' Working directory for process execution.
	''' </summary>
	Public Property WorkDir() As String
		Get
			Return m_WorkDir
		End Get
		Set(ByVal Value As String)
			m_WorkDir = Value
		End Set
	End Property

	''' <summary>
	''' When true then will write the standard output to a file in real-time
	''' Will also fire event ConsoleOutputEvent as new text is written to the console
	''' Define the path to the file using property ConsoleOutputFilePath; if not defined, the file
	''' will be created in the WorkDir (though, if WorkDir is blank, then will be created in the folder with the Program we're running)
	''' </summary>
	''' <remarks>If this is true, then no window will be shown, even if CreateNoWindow=False</remarks>
	Public Property WriteConsoleOutputToFile() As Boolean
		Get
			Return m_WriteConsoleOutputToFile
		End Get
		Set(ByVal value As Boolean)
			m_WriteConsoleOutputToFile = value
		End Set
	End Property
#End Region

#Region "Methods"
	''' <summary>
	''' Constructor
	''' </summary>
	''' <param name="WorkDir">Workdirectory for input/output files, if any</param>
	''' <remarks></remarks>
	Sub New(ByVal WorkDir As String)

		m_WorkDir = WorkDir

	End Sub

	''' <summary>
	''' Call this function to instruct this class to terminate the running program
	''' Will post an entry to the log
	''' </summary>
	Public Sub AbortProgramNow()
		AbortProgramNow(blnPostLogEntry:=True)
	End Sub

	''' <summary>
	''' Call this function to instruct this class to terminate the running program
	''' </summary>
	''' <param name="blnPostLogEntry">True if an entry should be posted to the log</param>
	''' <remarks></remarks>
	Public Sub AbortProgramNow(ByVal blnPostLogEntry As Boolean)
		m_AbortProgramNow = True
		m_AbortProgramPostLogEntry = blnPostLogEntry
	End Sub

	''' <summary>
	''' Runs a program and waits for it to exit
	''' </summary>
	''' <param name="ProgNameLoc">The path to the program to run</param>
	''' <param name="CmdLine">The arguments to pass to the program, for example /N=35</param>
	''' <param name="ProgName">The name of the program to use for the Window title</param>
	''' <returns>True if success, false if an error</returns>
	''' <remarks>Ignores the result code reported by the program</remarks>
	Public Function RunProgram(ByVal ProgNameLoc As String, ByVal CmdLine As String, ByVal ProgName As String) As Boolean
		Const useResCode As Boolean = False
		Return RunProgram(ProgNameLoc, CmdLine, ProgName, useResCode)
	End Function

	Public Function RunProgram(ByVal ProgNameLoc As String, ByVal CmdLine As String, ByVal ProgName As String, ByVal UseResCode As Boolean) As Boolean
		Const maxRuntime As Integer = 0
		Return RunProgram(ProgNameLoc, CmdLine, ProgName, UseResCode, maxRuntime)
	End Function

	''' <summary>
	''' Runs a program and waits for it to exit
	''' </summary>
	''' <param name="ProgNameLoc">The path to the program to run</param>
	''' <param name="CmdLine">The arguments to pass to the program, for example /N=35</param>
	''' <param name="ProgName">The name of the program to use for the Window title</param>
	''' <param name="UseResCode">If true, then returns False if the ProgRunner ExitCode is non-zero</param>
	''' <param name="MaxSeconds">If a positive number, then program execution will be aborted if the runtime exceeds MaxSeconds</param>
	''' <returns>True if success, false if an error</returns>
	''' <remarks>MaxRuntimeSeconds will be increased to 15 seconds if it is between 1 and 14 seconds</remarks>
	Public Function RunProgram(ByVal ProgNameLoc As String, ByVal CmdLine As String, ByVal ProgName As String, ByVal UseResCode As Boolean, ByVal MaxSeconds As Integer) As Boolean

		Dim dtStartTime As DateTime
		Dim blnRuntimeExceeded As Boolean
		Dim blnAbortLogged As Boolean

		' Require a minimum monitoring interval of 250 mseconds
		If m_MonitorInterval < 250 Then m_MonitorInterval = 250

		If MaxSeconds > 0 AndAlso MaxSeconds < 15 Then
			MaxSeconds = 15
		End If
		m_MaxRuntimeSeconds = MaxSeconds

		' Re-instantiate m_ProgRunner each time RunProgram is called since it is disposed of later in this function
		' Also necessary to avoid problems caching the console output
		m_ProgRunner = New PRISM.Processes.clsProgRunner
		With m_ProgRunner
			.Arguments = CmdLine
			.CreateNoWindow = m_CreateNoWindow
			.MonitoringInterval = m_MonitorInterval
			.Name = ProgName
			.Program = ProgNameLoc
			.Repeat = False
			.RepeatHoldOffTime = 0
			.WorkDir = m_WorkDir
			.CacheStandardOutput = m_CacheStandardOutput
			.EchoOutputToConsole = m_EchoOutputToConsole

			.WriteConsoleOutputToFile = m_WriteConsoleOutputToFile
			.ConsoleOutputFilePath = m_ConsoleOutputFilePath
		End With

		If m_DebugLevel >= 4 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "  ProgRunner.Arguments = " & m_ProgRunner.Arguments)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "  ProgRunner.Program = " & m_ProgRunner.Program)
		End If

        m_CachedConsoleErrors = String.Empty

		m_AbortProgramNow = False
		m_AbortProgramPostLogEntry = True
		blnRuntimeExceeded = False
		blnAbortLogged = False
		dtStartTime = DateTime.UtcNow

		Try
			' Start the program executing
			m_ProgRunner.StartAndMonitorProgram()

			' Loop until program is complete, or until m_MaxRuntimeSeconds seconds elapses
			While (m_ProgRunner.State <> PRISM.Processes.clsProgRunner.States.NotMonitoring)  ' And (ProgRunner.State <> 10)
				RaiseEvent LoopWaiting()
				Thread.Sleep(m_MonitorInterval)

				If m_MaxRuntimeSeconds > 0 Then
					If DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds > m_MaxRuntimeSeconds AndAlso Not m_AbortProgramNow Then
						m_AbortProgramNow = True
						blnRuntimeExceeded = True
						RaiseEvent Timeout()
					End If
				End If

				If m_ProgRunner.State = PRISM.Processes.clsProgRunner.States.StartingProcess AndAlso DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds > 30 AndAlso DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds < 90 Then
					' It has taken over 30 seconds for the thread to start
					' Try re-joining
					m_ProgRunner.JoinThreadNow()
				End If

				If m_AbortProgramNow Then
					If m_AbortProgramPostLogEntry AndAlso Not blnAbortLogged Then
						blnAbortLogged = True
						If blnRuntimeExceeded Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "  Aborting ProgRunner since " & m_MaxRuntimeSeconds & " seconds has elapsed")
						Else
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "  Aborting ProgRunner since AbortProgramNow() was called")
						End If
					End If
					m_ProgRunner.StopMonitoringProgram(Kill:=True)
				End If
			End While

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception running DOS program " & ProgNameLoc & "; " & clsGlobal.GetExceptionStackTrace(ex))
			m_ProgRunner = Nothing
			Return False
		End Try

		' Cache the exit code in m_ExitCode
		m_ExitCode = m_ProgRunner.ExitCode
		m_ProgRunner = Nothing

		If (UseResCode And m_ExitCode <> 0) Then
			If (m_AbortProgramNow AndAlso m_AbortProgramPostLogEntry) OrElse Not m_AbortProgramNow Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "  ProgRunner.ExitCode = " & m_ExitCode.ToString & " for Program = " & ProgNameLoc)
			End If
			Return False
		End If

		If m_AbortProgramNow Then
			Return False
		Else
			Return True
		End If

	End Function
#End Region

	Private Sub ProgRunner_ConsoleErrorEvent(ByVal NewText As String) Handles m_ProgRunner.ConsoleErrorEvent
        RaiseEvent ConsoleErrorEvent(NewText)
        If String.IsNullOrWhiteSpace(m_CachedConsoleErrors) Then
            m_CachedConsoleErrors = NewText
        Else
            m_CachedConsoleErrors &= Environment.NewLine & NewText
        End If        
		Console.WriteLine("Console error: " & Environment.NewLine & NewText)
	End Sub

	Private Sub ProgRunner_ConsoleOutputEvent(ByVal NewText As String) Handles m_ProgRunner.ConsoleOutputEvent
		RaiseEvent ConsoleOutputEvent(NewText)
	End Sub

	Private Sub ProgRunner_ProgChanged(ByVal obj As PRISM.Processes.clsProgRunner) Handles m_ProgRunner.ProgChanged
		' This event is ignored by this class
	End Sub

End Class


