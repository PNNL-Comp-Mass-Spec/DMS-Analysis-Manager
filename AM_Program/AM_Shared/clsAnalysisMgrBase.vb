Public MustInherit Class clsAnalysisMgrBase
    Inherits clsLoggerBase

    Private m_LastLockQueueWaitTimeLog As DateTime = Date.UtcNow
    Private m_LockQueueWaitTimeStart As DateTime = Date.UtcNow

    Protected m_FileTools As PRISM.Files.clsFileTools

    Protected m_message As String

    Private ReadOnly m_derivedClassName As String

    ''' <summary>
    ''' Constructor
    ''' </summary>
    ''' <param name="derivedClassName"></param>
    Public Sub New(derivedClassName As String)
        m_derivedClassName = derivedClassName
    End Sub

    Private Function IsLockQueueLogMessageNeeded(ByRef dtLockQueueWaitTimeStart As DateTime, ByRef dtLastLockQueueWaitTimeLog As DateTime) As Boolean

        Dim intWaitTimeLogIntervalSeconds As Integer

        If dtLockQueueWaitTimeStart = DateTime.MinValue Then dtLockQueueWaitTimeStart = Date.UtcNow()

        Select Case Date.UtcNow.Subtract(dtLockQueueWaitTimeStart).TotalMinutes
            Case Is >= 30
                intWaitTimeLogIntervalSeconds = 240
            Case Is >= 15
                intWaitTimeLogIntervalSeconds = 120
            Case Is >= 5
                intWaitTimeLogIntervalSeconds = 60
            Case Else
                intWaitTimeLogIntervalSeconds = 30
        End Select

        If Date.UtcNow.Subtract(dtLastLockQueueWaitTimeLog).TotalSeconds >= intWaitTimeLogIntervalSeconds Then
            Return True
        Else
            Return False
        End If

    End Function

    Protected Sub InitFileTools(mgrName As String, debugLevel As Short)
        ResetTimestampForQueueWaitTimeLogging()
        m_FileTools = New PRISM.Files.clsFileTools(mgrName, debugLevel)
        AddHandler m_FileTools.LockQueueTimedOut, AddressOf m_FileTools_LockQueueTimedOut
        AddHandler m_FileTools.LockQueueWaitComplete, AddressOf m_FileTools_LockQueueWaitComplete
        AddHandler m_FileTools.WaitingForLockQueue, AddressOf m_FileTools_WaitingForLockQueue
        AddHandler m_FileTools.DebugEvent, AddressOf m_FileTools_DebugEvent
        AddHandler m_FileTools.WarningEvent, AddressOf m_FileTools_WarningEvent
    End Sub

    ''' <summary>
    ''' Update m_message with an error message and record the error in the manager's log file
    ''' </summary>
    ''' <param name="errorMessage">Error message</param>
    ''' <param name="logToDb">When true, log the message to the database and the local log file</param>
    Protected Overrides Sub LogError(errorMessage As String, Optional logToDb As Boolean = False)
        m_message = clsGlobal.AppendToComment(m_message, errorMessage)
        MyBase.LogError(errorMessage, logToDb)
    End Sub

    ''' <summary>
    ''' Update m_message with an error message and record the error in the manager's log file
    ''' </summary>
    ''' <param name="errorMessage">Error message</param>
    ''' <param name="ex">Exception to log</param>
    Protected Overrides Sub LogError(errorMessage As String, ex As Exception)
        m_message = clsGlobal.AppendToComment(m_message, errorMessage)
        MyBase.LogError(errorMessage, ex)
    End Sub

    ''' <summary>
    ''' Update m_message with an error message and record the error in the manager's log file
    ''' Also write the detailed error message to the local log file
    ''' </summary>
    ''' <param name="errorMessage">Error message</param>
    ''' <param name="detailedMessage">Detailed error message</param>
    Protected Overloads Sub LogError(errorMessage As String, detailedMessage As String, Optional logToDb As Boolean = False)
        Me.LogError(errorMessage, logToDb)

        If Not String.IsNullOrEmpty(detailedMessage) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, detailedMessage)
            Console.ForegroundColor = ConsoleColor.Magenta
            Console.WriteLine(detailedMessage)
            Console.ResetColor()
        End If

    End Sub

    Protected Sub ResetTimestampForQueueWaitTimeLogging()
        m_LastLockQueueWaitTimeLog = Date.UtcNow
        m_LockQueueWaitTimeStart = Date.UtcNow
    End Sub


#Region "Event Handlers"

    Private Sub m_FileTools_DebugEvent(currentTask As String, taskDetail As String)
        If m_DebugLevel >= 1 Then
            Console.WriteLine(currentTask & "; " & taskDetail)
            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, currentTask & "; " & taskDetail)
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, currentTask)
            End If
        End If
    End Sub

    Private Sub m_FileTools_WarningEvent(warningMessage As String, warningDetail As String)
        If m_DebugLevel >= 1 Then
            Dim msg As String
            Console.WriteLine(warningMessage & "; " & warningDetail)
            If m_DebugLevel >= 2 Then
                msg = warningMessage & "; " & warningDetail
            Else
                msg = warningMessage
            End If
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg)
        End If
    End Sub

    Private Sub m_FileTools_LockQueueTimedOut(sourceFilePath As String, targetFilePath As String, waitTimeMinutes As Double)
        If m_DebugLevel >= 1 Then
            Dim msg = "Lockfile queue timed out after " & waitTimeMinutes.ToString("0") & " minutes " &
                "(" & m_derivedClassName & "); Source=" & sourceFilePath & ", Target=" & targetFilePath
            Console.WriteLine(msg)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg)
        End If
    End Sub

    Private Sub m_FileTools_LockQueueWaitComplete(sourceFilePath As String, targetFilePath As String, waitTimeMinutes As Double)
        If m_DebugLevel >= 1 AndAlso waitTimeMinutes >= 1 Then
            Dim msg = "Exited lockfile queue after " & waitTimeMinutes.ToString("0") & " minutes (" & m_derivedClassName & "); will now copy file"
            Console.WriteLine(msg)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg)
        End If
    End Sub

    Private Sub m_FileTools_WaitingForLockQueue(sourceFilePath As String, targetFilePath As String, backlogSourceMB As Integer, backlogTargetMB As Integer)

        If IsLockQueueLogMessageNeeded(m_LockQueueWaitTimeStart, m_LastLockQueueWaitTimeLog) Then
            m_LastLockQueueWaitTimeLog = Date.UtcNow
            If m_DebugLevel >= 1 Then
                Dim msg = "Waiting for lockfile queue to fall below threshold (" & m_derivedClassName & "); " &
                    "SourceBacklog=" & backlogSourceMB & " MB, " &
                    "TargetBacklog=" & backlogTargetMB & " MB, " &
                    "Source=" & sourceFilePath & ", Target=" & targetFilePath
                Console.WriteLine(msg)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg)
            End If
        End If

    End Sub

#End Region
End Class
