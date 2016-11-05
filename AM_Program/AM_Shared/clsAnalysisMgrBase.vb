Public MustInherit Class clsAnalysisMgrBase

    Protected m_DebugLevel As Short = 1

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
    Public Sub LogError(errorMessage As String)
        m_message = errorMessage
        Console.ForegroundColor = ConsoleColor.Red
        Console.WriteLine(errorMessage)
        Console.ResetColor()
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message)
    End Sub

    ''' <summary>
    ''' Update m_message with an error message and record the error in the manager's log file
    ''' </summary>
    ''' <param name="errorMessage">Error message</param>
    ''' <param name="ex">Exception to log</param>
    Public Sub LogError(errorMessage As String, ex As Exception)
        m_message = errorMessage
        ReportStatus(errorMessage, ex)
    End Sub

    ''' <summary>
    ''' Update m_message with an error message and record the error in the manager's log file
    ''' Also write the detailed error message to the local log file
    ''' </summary>
    ''' <param name="errorMessage">Error message</param>
    ''' <param name="detailedMessage">Detailed error message</param>
    Public Sub LogError(errorMessage As String, detailedMessage As String)
        m_message = errorMessage
        Console.ForegroundColor = ConsoleColor.Red
        If String.IsNullOrEmpty(detailedMessage) Then
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage)
            Console.WriteLine(errorMessage)
        Else
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, detailedMessage)
            Console.WriteLine(errorMessage)
            Console.ForegroundColor = ConsoleColor.Magenta
            Console.WriteLine(detailedMessage)
        End If
        Console.ResetColor()
    End Sub

    ''' <summary>
    ''' Shows information about an exception at the console and in the log file
    ''' Unlike LogErrors, does not update m_message
    ''' </summary>
    ''' <param name="errorMessage">Error message (do not include ex.message)</param>
    ''' <param name="ex">Exception</param>
    Public Sub ReportStatus(errorMessage As String, ex As Exception)
        Dim formattedError As String
        If errorMessage.EndsWith(ex.Message) Then
            formattedError = errorMessage
        Else
            formattedError = errorMessage & ": " & ex.Message
        End If
        Console.ForegroundColor = ConsoleColor.Red
        Console.WriteLine(formattedError)
        Console.ForegroundColor = ConsoleColor.Cyan
        Console.WriteLine(clsGlobal.GetExceptionStackTrace(ex))
        Console.ResetColor()
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, formattedError, ex)
    End Sub

    ''' <summary>
    ''' Show a status message at the console and optionally include in the log file
    ''' </summary>
    ''' <param name="statusMessage">Status message</param>
    ''' <param name="logFileDebugLevel">
    ''' Log level for whether to log to disk: 
    ''' 0 to always log
    ''' 1 to log if m_DebugLevel is >= 1
    ''' 2 to log if m_DebugLevel is >= 2
    ''' 10 to not log to disk
    ''' </param>
    ''' <param name="isError">True if this is an error</param>
    ''' <remarks>Unlike LogErrors, does not update m_message</remarks>
    Public Sub ReportStatus(statusMessage As String, Optional logFileDebugLevel As Integer = 0, Optional isError As Boolean = False)
        If isError Then
            Console.ForegroundColor = ConsoleColor.Red
            Console.WriteLine(statusMessage)
            Console.ResetColor()
        Else
            Console.WriteLine(statusMessage)
        End If

        If logFileDebugLevel < 10 AndAlso (logFileDebugLevel = 0 OrElse logFileDebugLevel <= m_DebugLevel) Then
            If isError Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, statusMessage)
            Else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, statusMessage)
            End If
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
