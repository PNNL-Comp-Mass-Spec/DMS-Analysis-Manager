'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Namespace AnalysisManagerBase

    Public Class clsRunDosProgram

        '*********************************************************************************************************
        'Provides a looping wrapper around a ProgRunner object for running command-line programs
        '*********************************************************************************************************

#Region "Module variables"
        Private m_CreateNoWindow As Boolean = True
        Private m_MonitorInterval As Integer = 2000  'msec
        Private m_WorkDir As String
        Private m_DebugLevel As Integer = 0
        Private m_ExitCode As Integer = 0

        Private m_CacheStandardOutput As Boolean = False
        Private m_EchoOutputToConsole As Boolean = True

        Private m_WriteConsoleOutputToFile As Boolean = False
        Private m_ConsoleOutputFilePath As String = String.Empty

        'Runs specified program
        Private WithEvents m_ProgRunner As PRISM.Processes.clsProgRunner = New PRISM.Processes.clsProgRunner

#End Region

#Region "Events"
        Public Event LoopWaiting()   'Class is waiting until next time it's due to check status of called program (good time for external processing)
        Public Event ConsoleOutputEvent(ByVal NewText As String)
        Public Event ConsoleErrorEvent(ByVal NewText As String)
#End Region

#Region "Properties"

        ''' <summary>
        ''' Text written to the Console by the external program (including carriage returns)
        ''' </summary>
        Public ReadOnly Property CachedConsoleOutput() As String
            Get
                Return m_ProgRunner.CachedConsoleOutput
            End Get
        End Property

        ''' <summary>
        ''' Any text written to the Error buffer by the external program
        ''' </summary>
        Public ReadOnly Property CachedConsoleError() As String
            Get
                Return m_ProgRunner.CachedConsoleError
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
        ''' How often (milliseconds) internal monitoring thread checks status of external program
        ''' Minimum allowed value is 50 milliseconds
        ''' </summary>
        Public Property MonitorInterval() As Integer     'msec
            Get
                Return m_MonitorInterval
            End Get
            Set(ByVal Value As Integer)
                m_MonitorInterval = Value
            End Set
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
        ''' 
        ''' </summary>
        ''' <param name="ProgNameLoc">The path to the program to run</param>
        ''' <param name="CmdLine">The arguments to pass to the program, for example /N=35</param>
        ''' <param name="ProgName">The name of the program to use for the Window title</param>
        ''' <param name="UseResCode">If true, then returns False if the ProgRunner ExitCode is non-zero</param>
        ''' <returns>True if success, false if an error</returns>
        ''' <remarks></remarks>
        Public Function RunProgram(ByVal ProgNameLoc As String, ByVal CmdLine As String, ByVal ProgName As String, _
         Optional ByVal UseResCode As Boolean = False) As Boolean

            'DAC debugging
            '		System.Threading.Thread.CurrentThread.Name = "RunProg"

            ' Require a minimum monitoring interval of 250 mseconds
            If m_MonitorInterval < 250 Then m_MonitorInterval = 250

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

            If m_DebugLevel > 3 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsRunDosProgram.RunProgram(), ProgRunner.Arguments = " & m_ProgRunner.Arguments)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsRunDosProgram.RunProgram(), ProgRunner.Program = " & m_ProgRunner.Program)
            End If

            'DAC debugging
            Debug.WriteLine("clsRunDOSProg.RunProgram, starting program, thread " & System.Threading.Thread.CurrentThread.Name)
            Try
                'Start the program executing
                m_ProgRunner.StartAndMonitorProgram()
                'loop until program is complete
                While (m_ProgRunner.State <> PRISM.Processes.clsProgRunner.States.NotMonitoring)  ' And (ProgRunner.State <> 10)
                    RaiseEvent LoopWaiting()
                    System.Threading.Thread.Sleep(m_MonitorInterval)
                End While
            Catch ex As Exception
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception running DOS program " & ProgNameLoc & "; " & clsGlobal.GetExceptionStackTrace(ex))
                Return False
            End Try

            ' Cache the exit code in m_ExitCode
            m_ExitCode = m_ProgRunner.ExitCode

            If (UseResCode And m_ProgRunner.ExitCode <> 0) Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsRunDosProgram.RunProgram(), Error: ProgRunner.ExitCode = " & m_ProgRunner.ExitCode.ToString & " for Program = " & ProgNameLoc)
                Return False
            Else
                Return True
            End If

            'DAC debugging
            Debug.WriteLine("clsRunDOSProg.RunProgram, program complete, thread " & System.Threading.Thread.CurrentThread.Name)
            Debug.WriteLine("Thread priority: " & System.Threading.Thread.CurrentThread.Priority)

            m_ProgRunner = Nothing

        End Function
#End Region

        Private Sub ProgRunner_ConsoleErrorEvent(ByVal NewText As String) Handles m_ProgRunner.ConsoleErrorEvent
            RaiseEvent ConsoleErrorEvent(NewText)
            Console.WriteLine("Console error: " & Environment.NewLine & NewText)
        End Sub

        Private Sub ProgRunner_ConsoleOutputEvent(ByVal NewText As String) Handles m_ProgRunner.ConsoleOutputEvent
            RaiseEvent ConsoleOutputEvent(NewText)
        End Sub

        Private Sub ProgRunner_ProgChanged(ByVal obj As PRISM.Processes.clsProgRunner) Handles m_ProgRunner.ProgChanged
            ' This event is ignored by this class
        End Sub

    End Class

End Namespace
