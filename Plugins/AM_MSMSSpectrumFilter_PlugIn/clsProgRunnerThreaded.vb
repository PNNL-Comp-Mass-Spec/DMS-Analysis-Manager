Option Strict On

' This class runs a single program as an external process
' and monitors it with an internal thread
'
''' <summary>
''' This class runs a single program as an external process and monitors it with an internal thread.
''' </summary>
Public Class clsProgRunnerThreaded

#Region "Constants and Enums"

    ''' <summary>
    ''' clsProgRunner states
    ''' </summary>
    Public Enum States
        NotMonitoring
        Monitoring
        Waiting
        CleaningUp
    End Enum
#End Region

#Region "Classwide Variables"

    ''' <summary>
    ''' True for logging behavior, else false.
    ''' </summary>
    Private m_NotifyOnException As Boolean
    ''' <summary>
    ''' True for logging behavior, else false.
    ''' </summary>
    Private m_NotifyOnEvent As Boolean

    ' overall state of this object
    Private m_state As States = States.NotMonitoring

    ''' <summary>
    ''' Used to start and monitor the external program.
    ''' </summary>
    Private m_Process As New Process

    ''' <summary>
    ''' The process id of the currently running incarnation of the external program.
    ''' </summary>
    Private m_pid As Integer

    ''' <summary>
    ''' The internal thread used to run the monitoring code.
    ''' </summary>
    ''' <remarks>
    ''' That starts and monitors the external program
    ''' </remarks>
    Private m_Thread As System.Threading.Thread

    ''' <summary>
    ''' Flag that tells internal thread to quit monitoring external program and exit.
    ''' </summary>
    Private m_doCleanup As Boolean = False

    ''' <summary>
    ''' The interval for monitoring thread to wake up and check m_doCleanup.
    ''' </summary>
    Private m_monitorInterval As Integer = 5000 ' (milliseconds)

    ''' <summary>
    ''' Exit code returned by completed process.
    ''' </summary>
    Private m_ExitCode As Integer

    ''' <summary>
    ''' Parameters for external program.
    ''' </summary>
    Private m_name As String
    Private m_ProgName As String
    Private m_ProgArgs As String
    Private m_repeat As Boolean = False
    Private m_holdOffTime As Integer = 3000
    Private m_WorkDir As String
    Private m_CreateNoWindow As Boolean
    Private m_WindowStyle As System.Diagnostics.ProcessWindowStyle

    Private m_CacheStandardOutput As Boolean
    Private m_EchoOutputToConsole As Boolean

    ''' <summary>
    ''' Caches the text written to the Console by the external program
    ''' </summary>
    Private m_CachedConsoleOutput As System.Text.StringBuilder

    ''' <summary>
    ''' Caches the text written to the Error buffer by the external program
    ''' </summary>
    Private m_CachedConsoleError As System.Text.StringBuilder

#End Region

#Region "Events"

    ''' <summary>
    ''' This event is raised whenever the state property changes.
    ''' </summary>
    Public Event ProgChanged(ByVal obj As clsProgRunnerThreaded)

    ''' <summary>
    ''' This event is raised when the external program writes text to the console
    ''' </summary>
    ''' <param name="NewText"></param>
    ''' <remarks></remarks>
    Public Event ConsoleOutputEvent(ByVal NewText As String)

    ''' <summary>
    ''' This event is raised when the external program writes text to the console's error stream
    ''' </summary>
    ''' <param name="NewText"></param>
    ''' <remarks></remarks>
    Public Event ConsoleErrorEvent(ByVal NewText As String)

#End Region

#Region "Properties"

    ''' <summary>
    ''' Arguments supplied to external program when it is run.
    ''' </summary>
    Public Property Arguments() As String
        Get
            Return m_ProgArgs
        End Get
        Set(ByVal Value As String)
            m_ProgArgs = Value
        End Set
    End Property

    ''' <summary>
    ''' Text written to the Console by the external program (including carriage returns)
    ''' </summary>
    Public ReadOnly Property CachedConsoleOutput() As String
        Get
            If m_CachedConsoleOutput Is Nothing Then
                Return String.Empty
            Else
                Return m_CachedConsoleOutput.ToString
            End If
        End Get
    End Property

    ''' <summary>
    ''' Any text written to the Error buffer by the external program
    ''' </summary>
    Public ReadOnly Property CachedConsoleError() As String
        Get
            If m_CachedConsoleError Is Nothing Then
                Return String.Empty
            Else
                Return m_CachedConsoleError.ToString
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
    Public Property MonitoringInterval() As Integer
        Get
            Return m_monitorInterval
        End Get
        Set(ByVal Value As Integer)
            If Value < 50 Then Value = 50
            m_monitorInterval = Value
        End Set
    End Property

    ''' <summary>
    ''' Name of this progrunner.
    ''' </summary>
    Public Property Name() As String
        Get
            Return m_name
        End Get
        Set(ByVal Value As String)
            m_name = Value
        End Set
    End Property


    ''' <summary>
    ''' Process id of currently running external program's process.
    ''' </summary>
    Public ReadOnly Property PID() As Integer
        Get
            Return m_pid
        End Get
    End Property

    ''' <summary>
    ''' External program that prog runner will run.
    ''' </summary>
    Public Property Program() As String
        Get
            Return m_ProgName
        End Get
        Set(ByVal Value As String)
            m_ProgName = Value
        End Set
    End Property

    ''' <summary>
    ''' Whether prog runner will restart external program after it exits.
    ''' </summary>
    Public Property Repeat() As Boolean
        Get
            Return m_repeat
        End Get
        Set(ByVal Value As Boolean)
            m_repeat = Value
        End Set
    End Property

    ''' <summary>
    ''' Time (seconds) that prog runner waits to restart external program after it exits.
    ''' </summary>
    Public Property RepeatHoldOffTime() As Double
        Get
            Return m_holdOffTime / 1000.0
        End Get
        Set(ByVal Value As Double)
            m_holdOffTime = CType(Value * 1000, Integer)
        End Set
    End Property

    ''' <summary>
    ''' Current state of prog runner (as number).
    ''' </summary>
    Public ReadOnly Property State() As States
        Get
            Return m_state
        End Get
    End Property

    ''' <summary>
    ''' Current state of prog runner (as descriptive name).
    ''' </summary>
    Public ReadOnly Property StateName() As String
        Get
            Select Case m_state
                Case States.NotMonitoring
                    StateName = "not monitoring"
                Case States.Monitoring
                    StateName = "monitoring"
                Case States.Waiting
                    StateName = "waiting to restart"
                Case States.CleaningUp
                    StateName = "cleaning up"
                Case Else
                    StateName = "???"
            End Select
        End Get
    End Property

    ''' <summary>
    ''' Window style to use when CreateNoWindow is False.
    ''' </summary>
    Public Property WindowStyle() As System.Diagnostics.ProcessWindowStyle
        Get
            Return m_WindowStyle
        End Get
        Set(ByVal Value As System.Diagnostics.ProcessWindowStyle)
            m_WindowStyle = Value
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

#End Region

#Region "Methods"

    ''' <summary>
    ''' Initializes a new instance of the clsProgRunner class.
    ''' </summary>
    Public Sub New()
        m_WorkDir = ""
        m_CreateNoWindow = False
        m_ExitCode = -12354  'Unreasonable value in case I missed setting it somewhere
        m_NotifyOnEvent = True
        m_NotifyOnException = True
        m_CacheStandardOutput = False
        m_EchoOutputToConsole = True
    End Sub

    ''' <summary>
    ''' Clears any console output text that is currently cached
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub ClearCachedConsoleOutput()

        If m_CachedConsoleOutput Is Nothing Then
            m_CachedConsoleOutput = New System.Text.StringBuilder
        Else
            m_CachedConsoleOutput.Length = 0
        End If

    End Sub

    ''' <summary>
    ''' Clears any console error text that is currently cached
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub ClearCachedConsoleError()

        If m_CachedConsoleError Is Nothing Then
            m_CachedConsoleError = New System.Text.StringBuilder
        Else
            m_CachedConsoleError.Length = 0
        End If

    End Sub

    ''' <summary>
    ''' Asynchronously handles the console output from the process running by m_Process
    ''' </summary>
    Private Sub ConsoleOutputHandler(ByVal sendingProcess As Object, _
                                     ByVal outLine As DataReceivedEventArgs)

        ' Collect the console output.
        If Not String.IsNullOrEmpty(outLine.Data) Then

            RaiseEvent ConsoleOutputEvent(outLine.Data)

            If m_EchoOutputToConsole Then
                Console.WriteLine(outLine.Data)
            End If

            If m_CacheStandardOutput Then
                ' Add the text to the collected output.
                m_CachedConsoleOutput.AppendLine(outLine.Data)
            End If
        End If
    End Sub

    ''' <summary>
    ''' Handles any new data in the console output and console error streams
    ''' </summary>
    Private Sub HandleOutputStreams(ByRef srConsoleError As System.IO.StreamReader)

        Dim strNewText As String

        If Not srConsoleError Is Nothing AndAlso srConsoleError.Peek >= 0 Then
            strNewText = srConsoleError.ReadToEnd

            RaiseEvent ConsoleErrorEvent(strNewText)

            If Not m_CachedConsoleError Is Nothing Then
                m_CachedConsoleError.Append(strNewText)
            End If
        End If

    End Sub

    Private Sub RaiseConditionalProgChangedEvent(ByVal obj As clsProgRunnerThreaded)
        If m_NotifyOnEvent Then
            RaiseEvent ProgChanged(obj)
        End If
    End Sub

    ''' <summary>
    ''' Start program as external process and monitor its state.
    ''' </summary>
    Private Sub Start()

		Dim srConsoleError As System.IO.StreamReader = Nothing
        Dim blnStandardOutputRedirected As Boolean

        ' set up parameters for external process
        '
        With m_Process.StartInfo
            .FileName = m_ProgName
            .WorkingDirectory = m_WorkDir
            .Arguments = m_ProgArgs
            .CreateNoWindow = m_CreateNoWindow
            If .CreateNoWindow Then
                .WindowStyle = ProcessWindowStyle.Hidden
            Else
                .WindowStyle = m_WindowStyle
            End If

            If .CreateNoWindow OrElse m_CacheStandardOutput Then
                .UseShellExecute = False
                .RedirectStandardOutput = True
                .RedirectStandardError = True
                blnStandardOutputRedirected = True
            Else
                .UseShellExecute = True
                .RedirectStandardOutput = False
                blnStandardOutputRedirected = False
            End If

        End With

        If Not System.IO.File.Exists(m_Process.StartInfo.FileName) Then
            ThrowConditionalException(New Exception("Process filename " & m_Process.StartInfo.FileName & " not found."), _
            "clsProgRunner m_ProgName was not set correctly.")
            Exit Sub
        End If

        If Not System.IO.Directory.Exists(m_Process.StartInfo.WorkingDirectory) Then
            ThrowConditionalException(New Exception("Process working directory " & m_Process.StartInfo.WorkingDirectory & " not found."), _
            "clsProgRunner m_WorkDir was not set correctly.")
            Exit Sub
        End If

        If blnStandardOutputRedirected Then
            ' Add an event handler to asynchronously read the console output
            AddHandler m_Process.OutputDataReceived, AddressOf ConsoleOutputHandler
        End If

        ' Make sure the cached output StringBuilders are initialized
        ClearCachedConsoleOutput()
        ClearCachedConsoleError()

        Do
            ' start the program as an external process
            '
            Try
                m_Process.Start()
            Catch ex As Exception
                ThrowConditionalException(ex, "Problem starting process. Parameters: " & _
                    m_Process.StartInfo.WorkingDirectory & m_Process.StartInfo.FileName & " " & _
                    m_Process.StartInfo.Arguments & ".")
                Exit Sub
            End Try

            m_pid = m_Process.Id

            If blnStandardOutputRedirected Then
                m_Process.BeginOutputReadLine()

                ' Attach a StreamReader to m_Process.StandardError 
                srConsoleError = m_Process.StandardError

                ' Do not attach a reader to m_Process.StandardOutput
                ' since we are asynchronously reading the console output
            End If

            RaiseConditionalProgChangedEvent(Me)

            ' wait for program to exit (loop on interval)
            ' until external process exits or class is commanded
            ' to stop monitoring the process (m_doCleanup = true)
            '
            While Not (m_doCleanup Or m_Process.HasExited)
                m_Process.WaitForExit(m_monitorInterval)
            End While

            ' need to free up resources used to keep
            ' track of the external process
            '
            m_pid = 0
            m_ExitCode = m_Process.ExitCode

            If blnStandardOutputRedirected Then
                ' Read any console error text using srConsoleError
                HandleOutputStreams(srConsoleError)

                If Not srConsoleError Is Nothing Then srConsoleError.Close()
            End If

            m_Process.Close()

            ' decide whether or not to repeat starting
            ' the external process again, or quit
            '
            If m_repeat And Not m_doCleanup Then
                ' repeat starting the process
                ' after waiting for minimum hold off time interval
                '
                m_state = States.Waiting

                RaiseConditionalProgChangedEvent(Me)

                System.Threading.Thread.Sleep(m_holdOffTime)

                m_state = States.Monitoring
            Else
                ' don't repeat starting the process - just quit
                '
                m_state = States.NotMonitoring
                RaiseConditionalProgChangedEvent(Me)
                Exit Do
            End If
        Loop

    End Sub

    ''' <summary>
    ''' Creates a new thread and starts code that runs and monitors a program in it.
    ''' </summary>
    Public Sub StartAndMonitorProgram()
        If m_state = States.NotMonitoring Then
            m_state = States.Monitoring
            m_doCleanup = False

            ' arrange to start the program as an external process
            ' and monitor it in a separate internal thread
            '
            Try
                Dim m_ThreadStart As New System.Threading.ThreadStart(AddressOf Me.Start)
                m_Thread = New System.Threading.Thread(m_ThreadStart)
                m_Thread.Start()
            Catch ex As Exception
                ThrowConditionalException(ex, "Caught exception while trying to start thread.")

            End Try
        End If
    End Sub

    ''' <summary>
    ''' Causes monitoring thread to exit on its next monitoring cycle.
    ''' </summary>
    Public Sub StopMonitoringProgram(Optional ByVal Kill As Boolean = False)

        If m_state = States.Monitoring And Kill Then  'Program is running, kill it and abort thread
            Try
                m_Process.Kill()
                m_Thread.Abort()  'DAC added
            Catch ex As System.Threading.ThreadAbortException
                ThrowConditionalException(CType(ex, Exception), "Caught ThreadAbortException while trying to abort thread.")
            Catch ex As System.ComponentModel.Win32Exception
                ThrowConditionalException(CType(ex, Exception), "Caught Win32Exception while trying to kill thread.")
            Catch ex As System.InvalidOperationException
                ThrowConditionalException(CType(ex, Exception), "Caught InvalidOperationException while trying to kill thread.")
            Catch ex As System.SystemException
                ThrowConditionalException(CType(ex, Exception), "Caught SystemException while trying to kill thread.")
            Catch ex As Exception
                ThrowConditionalException(CType(ex, Exception), "Caught Exception while trying to kill or abort thread.")
            End Try
        End If

        '********************************************************************************************************
        '	DAC added
        '********************************************************************************************************
        If m_state = States.Waiting And Kill Then  'Program not running, just abort thread
            Try
                m_Thread.Abort()
            Catch ex As System.Threading.ThreadAbortException
                ThrowConditionalException(CType(ex, Exception), "Caught ThreadAbortException while trying to abort thread.")
            Catch ex As Exception
                ThrowConditionalException(CType(ex, Exception), "Caught exception while trying to abort thread.")
            End Try
        End If
        '********************************************************************************************************
        '	DAC addition end
        '********************************************************************************************************

        If m_state = States.Monitoring Or m_state = States.Waiting Then
            m_state = States.CleaningUp
            m_doCleanup = True
            Try
                m_Thread.Join()
            Catch ex As System.Threading.ThreadStateException
                ThrowConditionalException(CType(ex, Exception), "Caught ThreadStateException while trying to join thread.")
            Catch ex As System.Threading.ThreadInterruptedException
                ThrowConditionalException(CType(ex, Exception), "Caught ThreadInterruptedException while trying to join thread.")
            Catch ex As Exception
                ThrowConditionalException(CType(ex, Exception), "Caught exception while trying to join thread.")
            End Try
            m_state = States.NotMonitoring
        End If
    End Sub

    Private Sub ThrowConditionalException(ByRef ex As Exception, ByVal loggerMessage As String)
        If m_NotifyOnException Then
            Throw ex
        End If
    End Sub

#End Region

End Class
