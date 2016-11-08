'Wraps Eric Strittmatter dll for performing cal operation on QTOF data
Imports System.IO
Imports System.Timers
Imports System.Threading

Public Class clsEricAgilTOFCal

#Region "DLL and API calls"
    Private Declare Function EricATOFFnc Lib "EricATOF.dll" (ByVal file As String, ByVal out As String, _
     ByRef hndl As Integer) As Short

    Private Declare Sub EricATOFVersion Lib "EricATOF.dll" (ByRef version As Byte)

    Private Declare Function WaitForSingleObject Lib "kernel32.dll" (ByVal hndl As Integer, _
     ByVal ufail As Integer) As Integer

    Private Declare Function TerminateThread Lib "kernel32.dll" (ByVal hndl As Integer, _
     ByVal ufail As Integer) As Integer
#End Region

#Region "Module constants and variables"
    'Constants
    Private Const WAIT_TIMEOUT As Integer = 258
    Private Const WAIT_SIGNALED As Integer = 0
    'Variables
    Private m_TimedOut As Boolean = False
    Private WithEvents m_Timer As System.Timers.Timer
    Private m_TimeoutSetting As Single = 10.0	'Value in seconds
#End Region

    Public Property TimeoutSetting() As Single
        'Value is in seconds
        Get
            Return m_TimeoutSetting
        End Get
        Set(ByVal Value As Single)
            m_TimeoutSetting = Value
        End Set
    End Property

    Public Overloads Function PerformCal(ByVal InputFile As String) As Boolean
        'Version permitting output file to be unspecified
        Return ReallyPerformCal(InputFile, CStr(vbNull))
    End Function

    Public Overloads Function PerformCal(ByVal InputFile As String, ByVal OutputFile As String) As Boolean
        'Version requiring input and output files to be specified
        Return ReallyPerformCal(InputFile, OutputFile)
    End Function

    Private Function ReallyPerformCal(ByVal InputFile As String, ByVal OutputFile As String) As Boolean

        Dim Hndl As Integer
        Dim Rtn As Integer
        Dim Fi As FileInfo

        'Verify existence and type of input file
        If File.Exists(InputFile) Then
            'Verify input file type
            Fi = New FileInfo(InputFile)
            If Fi.Extension.ToLower <> ".pek" Then Return False
        Else
            Return False
        End If

        'Set up the timer that prevents infinite wait
        m_Timer = New Timers.Timer
        m_Timer.Enabled = False
        m_Timer.Interval = m_TimeoutSetting * 1000.0		'Convert interval to milliseconds

        'Start the cal process
        Try
            m_TimedOut = False
            m_Timer.Enabled = True
            EricATOFFnc(InputFile, OutputFile, Hndl)

            'Wait for the calibration to exit
            Do
                Thread.Sleep(1000)				'Delay for 1 second
                Rtn = WaitForSingleObject(Hndl, 10)
                '' Application.DoEvents() '' grk 8/13/2005
            Loop While (Rtn = WAIT_TIMEOUT) And (Not m_TimedOut)

            'Check for timeout
            If m_TimedOut Then
                TerminateThread(Hndl, 10)
                Return False
            Else
                Return True
            End If
        Catch Err As Exception
            Return False
        End Try

    End Function

    Private Sub m_Timer_Elapsed(ByVal sender As Object, ByVal e As System.Timers.ElapsedEventArgs) Handles m_Timer.Elapsed
        m_TimedOut = True
    End Sub

End Class
