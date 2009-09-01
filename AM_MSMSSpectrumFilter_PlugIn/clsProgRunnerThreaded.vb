Option Strict On

' This class runs a single program as an external process
' and monitors it with an internal thread
'
''' <summary>
''' This class runs a single program as an external process and monitors it with an internal thread.
''' </summary>
Public Class clsProgRunnerThreaded

    ''' <summary>
    ''' clsProgRunnerThreaded states
    ''' </summary>
    Public Enum States
        NotMonitoring
        Monitoring
        Waiting
        CleaningUp
    End Enum

    ''' <summary>
    ''' True for logging behavior, else false.
    ''' </summary>
    Private m_NotifyOnException As Boolean
    ''' <summary>
    ''' True for logging behavior, else false.
    ''' </summary>
    Private m_NotifyOnEvent As Boolean

    ''' <summary>
    ''' This event is raised whenever the state property changes.
    ''' </summary>
    Public Event ProgChanged(ByVal obj As clsProgRunnerThreaded)

    ' overall state of this object
    Private m_state As States = States.NotMonitoring

    ''' <summary>
    ''' Used to start and monitor the external program.
    ''' </summary>
    Private m_Process As New System.Diagnostics.Process

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
    Private m_monitorInterval As Integer = 5000 ' (miliseconds)

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

    ''' <summary>
    ''' How often (milliseconds) internal monitoring thread checks status of external program
    ''' </summary>
    Public Property MonitoringInterval() As Integer
        Get
            Return m_monitorInterval
        End Get
        Set(ByVal Value As Integer)
            m_monitorInterval = Value
        End Set
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
    ''' Process id of currently running external program's process.
    ''' </summary>
    Public ReadOnly Property PID() As Integer
        Get
            Return m_pid
        End Get
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
    ''' Exit code when process completes.
    ''' </summary>
    Public ReadOnly Property ExitCode() As Integer
        Get
            Return m_ExitCode
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
    ''' Determine if window should be displayed.
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
    ''' Start program as external process and monitor its state.
    ''' </summary>
    Private Sub Start()
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
        End With

        If Not System.IO.File.Exists(m_Process.StartInfo.FileName) Then
            ThrowConditionalException(New Exception("Process filename " & m_Process.StartInfo.FileName & " not found."), _
            "clsProgRunnerThreaded m_ProgName was not set correctly.")
            Exit Sub
        End If

        If Not System.IO.Directory.Exists(m_Process.StartInfo.WorkingDirectory) Then
            ThrowConditionalException(New Exception("Process working directory " & m_Process.StartInfo.WorkingDirectory & " not found."), _
            "clsProgRunnerThreaded m_WorkDir was not set correctly.")
            Exit Sub
        End If

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

    ''' <summary>
    ''' Initializes a new instance of the clsProgRunnerThreaded class.
    ''' </summary>
    Public Sub New()
        m_WorkDir = ""
        m_CreateNoWindow = False
        m_ExitCode = -12354  'Unreasonable value in case I missed setting it somewhere
        m_NotifyOnEvent = True
        m_NotifyOnException = True
    End Sub

    Private Sub RaiseConditionalProgChangedEvent(ByVal obj As clsProgRunnerThreaded)
        If m_NotifyOnEvent Then
            RaiseEvent ProgChanged(obj)
        End If
    End Sub

    Private Sub ThrowConditionalException(ByRef ex As Exception, ByVal loggerMessage As String)
        If m_NotifyOnException Then
            Throw ex
        End If
    End Sub
End Class

