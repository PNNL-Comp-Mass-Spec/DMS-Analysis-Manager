Public MustInherit Class clsAnalysisMgrBase

    Protected m_DebugLevel As Short = 1

    Private m_LastLockQueueWaitTimeLog As DateTime = DateTime.UtcNow
    Private m_LockQueueWaitTimeStart As DateTime = DateTime.UtcNow

    Protected m_FileTools As PRISM.Files.clsFileTools

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

        If dtLockQueueWaitTimeStart = DateTime.MinValue Then dtLockQueueWaitTimeStart = DateTime.UtcNow()

        Select Case DateTime.UtcNow.Subtract(dtLockQueueWaitTimeStart).TotalMinutes
            Case Is >= 30
                intWaitTimeLogIntervalSeconds = 240
            Case Is >= 15
                intWaitTimeLogIntervalSeconds = 120
            Case Is >= 5
                intWaitTimeLogIntervalSeconds = 60
            Case Else
                intWaitTimeLogIntervalSeconds = 30
        End Select

        If DateTime.UtcNow.Subtract(dtLastLockQueueWaitTimeLog).TotalSeconds >= intWaitTimeLogIntervalSeconds Then
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

    Protected Sub ResetTimestampForQueueWaitTimeLogging()
        m_LastLockQueueWaitTimeLog = DateTime.UtcNow
        m_LockQueueWaitTimeStart = DateTime.UtcNow
    End Sub


#Region "Event Handlers"

    Private Sub m_FileTools_DebugEvent(currentTask As String, taskDetail As String)
        If m_DebugLevel >= 1 Then
            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, currentTask & "; " & taskDetail)
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, currentTask)
            End If
        End If
    End Sub

    Private Sub m_FileTools_WarningEvent(warningMessage As String, warningDetail As String)
        If m_DebugLevel >= 1 Then
            If m_DebugLevel >= 2 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, warningMessage & "; " & warningDetail)
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, warningMessage)
            End If
        End If
    End Sub

    Private Sub m_FileTools_LockQueueTimedOut(sourceFilePath As String, targetFilePath As String, waitTimeMinutes As Double)
        If m_DebugLevel >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                                 "Locked queue timed out after " & waitTimeMinutes.ToString("0") & " minutes (" & m_derivedClassName & "); " &
                                 "Source=" & sourceFilePath & ", Target=" & targetFilePath)
        End If
    End Sub

    Private Sub m_FileTools_LockQueueWaitComplete(sourceFilePath As String, targetFilePath As String, waitTimeMinutes As Double)
        If m_DebugLevel >= 1 AndAlso waitTimeMinutes >= 1 Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                 "Exited lockfile queue after " & waitTimeMinutes.ToString("0") & " minutes (" & m_derivedClassName & "); will now copy file")
        End If
    End Sub

    Private Sub m_FileTools_WaitingForLockQueue(sourceFilePath As String, targetFilePath As String, backlogSourceMB As Integer, backlogTargetMB As Integer)

        If IsLockQueueLogMessageNeeded(m_LockQueueWaitTimeStart, m_LastLockQueueWaitTimeLog) Then
            m_LastLockQueueWaitTimeLog = DateTime.UtcNow
            If m_DebugLevel >= 1 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                     "Waiting for lockfile queue to fall below threshold (" & m_derivedClassName & "); " &
                                     "SourceBacklog=" & backlogSourceMB & " MB, " &
                                     "TargetBacklog=" & backlogTargetMB & " MB, " &
                                     "Source=" & sourceFilePath & ", Target=" & targetFilePath)
            End If
        End If

    End Sub

#End Region
End Class
