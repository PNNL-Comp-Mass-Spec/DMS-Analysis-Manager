'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 05/09/2008
'*********************************************************************************************************

Imports PRISM.Logging

Namespace AnalysisManagerBase

	Public Class clsRunDosProgram

		'*********************************************************************************************************
		'Provides a looping wrapper around a ProgRunner object for running command-line programs
		'*********************************************************************************************************

#Region "Module variables"
		Private m_CreateNoWindow As Boolean = True
		Private m_MonitorInterval As Integer = 2000	 'msec
		Private m_Logger As ILogger
		Private m_WorkDir As String
        Private m_DebugLevel As Integer = 0
        Private m_ExitCode As Integer = 0
#End Region

#Region "Events"
        Public Event LoopWaiting()   'Class is waiting until next time it's due to check status of called program (good time for external processing)
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

        Public ReadOnly Property ExitCode() As Integer
            Get
                Return m_ExitCode
            End Get
        End Property
        Public Property MonitorInterval() As Integer     'msec
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

#Region "Methods"
        ''' <summary>
        ''' Constructor
        ''' </summary>
        ''' <param name="Logger">Logging object</param>
        ''' <param name="WorkDir">Workdirectory for input/output files, if any</param>
        ''' <remarks></remarks>
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

            ' Cache the exit code in m_ExitCode
            m_ExitCode = ProgRunner.ExitCode

            If ProgRunner.State = 10 Then
                m_Logger.PostEntry("clsAnalysisToolRunnerSeqBase.RunProgram(), Error: Progrunner.State = 10", _
                 ILogger.logMsgType.logError, True)
                Return False

            ElseIf (UseResCode And ProgRunner.ExitCode <> 0) Then
                m_Logger.PostEntry("clsAnalysisToolRunnerSeqBase.RunProgram(), Error: ProgRunner.ExitCode = " & ProgRunner.ExitCode.ToString, _
                 ILogger.logMsgType.logError, True)
                Return False

            Else
                Return True
            End If

            'DAC debugging
            Debug.WriteLine("clsRunDOSProg.RunProgram, program complete, thread " & System.Threading.Thread.CurrentThread.Name)
            Debug.WriteLine("Thread priority: " & System.Threading.Thread.CurrentThread.Priority)

            ProgRunner = Nothing

        End Function
#End Region

	End Class

End Namespace
