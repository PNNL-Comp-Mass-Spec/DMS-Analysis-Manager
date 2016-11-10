
Public MustInherit Class clsLoggerBase

    ''' <summary>
    ''' Debug level
    ''' Values from 0 (minimum output) to 5 (max detail)
    ''' </summary>
    Protected m_DebugLevel As Short = 1

    ''' <summary>
    ''' Log an error message
    ''' </summary>
    ''' <param name="errorMessage">Error message</param>
    ''' <param name="logToDb">When true, log the message to the database and the local log file</param>
    Protected Overridable Sub LogError(errorMessage As String, Optional logToDb As Boolean = False)
        Console.ForegroundColor = ConsoleColor.Red
        Console.WriteLine(errorMessage)
        Console.ResetColor()

        Dim loggerType As clsLogTools.LoggerTypes
        If logToDb Then
            loggerType = clsLogTools.LoggerTypes.LogDb
        Else
            loggerType = clsLogTools.LoggerTypes.LogFile
        End If

        clsLogTools.WriteLog(loggerType, clsLogTools.LogLevels.ERROR, errorMessage)
    End Sub

    ''' <summary>
    ''' Log an error message and exception
    ''' </summary>
    ''' <param name="errorMessage">Error message</param>
    ''' <param name="ex">Exception to log</param>
    Protected Overridable Sub LogError(errorMessage As String, ex As Exception)
        ReportStatus(errorMessage, ex)
    End Sub

    ''' <summary>
    ''' Log a warning message
    ''' </summary>
    ''' <param name="warningMessage">Warning message</param>
    Protected Sub LogWarning(warningMessage As String)
        Console.ForegroundColor = ConsoleColor.Yellow
        Console.WriteLine(warningMessage)
        Console.ResetColor()
        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, warningMessage)
    End Sub

    ''' <summary>
    ''' Shows information about an exception at the console and in the log file
    ''' </summary>
    ''' <param name="errorMessage">Error message (do not include ex.message)</param>
    ''' <param name="ex">Exception</param>
    Protected Sub ReportStatus(errorMessage As String, ex As Exception)
        Dim formattedError As String
        If errorMessage.EndsWith(ex.Message) Then
            formattedError = errorMessage
        Else
            formattedError = errorMessage & ": " & ex.Message
        End If
        Console.ForegroundColor = ConsoleColor.Red
        Console.WriteLine(formattedError)
        Console.ForegroundColor = ConsoleColor.Cyan
        Console.WriteLine(clsGlobal.GetExceptionStackTrace(ex, True))
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
    Protected Sub ReportStatus(statusMessage As String, Optional logFileDebugLevel As Integer = 0, Optional isError As Boolean = False)
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

End Class
