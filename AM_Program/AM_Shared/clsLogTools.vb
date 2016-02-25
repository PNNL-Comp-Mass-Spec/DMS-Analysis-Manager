'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2009, Battelle Memorial Institute
' Created 01/01/2009
'
'*********************************************************************************************************

Option Strict On

Imports log4net.Appender
Imports log4net
Imports log4net.Util.TypeConverters

'This assembly attribute tells Log4Net where to find the config file
<Assembly: log4net.Config.XmlConfigurator(ConfigFile:="Logging.config", Watch:=True)> 

Public Class clsLogTools

    '*********************************************************************************************************
    ' Class for handling logging via Log4Net
    '*********************************************************************************************************

#Region "Constants"
    Public Const DB_LOGGER_MGR_CONTROL = "MgrControlDbDefinedAppender"
    Public Const DB_LOGGER_NO_MGR_CONTROL_PARAMS = "DbAppenderBeforeMgrControlParams"

    Private Const LOG_FILE_APPENDER = "FileAppender"
#End Region

#Region "Enums"
    Public Enum LogLevels
        DEBUG = 5
        INFO = 4
        WARN = 3
        [ERROR] = 2
        FATAL = 1
    End Enum

    Public Enum LoggerTypes
        LogFile
        LogDb
        LogSystem
    End Enum
#End Region

#Region "Module variables"
    Private Shared ReadOnly m_FileLogger As ILog = LogManager.GetLogger("FileLogger")
    Private Shared ReadOnly m_DbLogger As ILog = LogManager.GetLogger("DbLogger")
    Private Shared ReadOnly m_SysLogger As ILog = LogManager.GetLogger("SysLogger")

    Private Shared m_FileDate As String = ""
    Private Shared m_BaseFileName As String = ""
    Private Shared m_FileAppender As FileAppender

    Private Shared m_MostRecentErrorMessage As String = String.Empty
#End Region

#Region "Properties"

    ''' <summary>
    ''' File path for the current log file used by the FileAppender
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Shared ReadOnly Property CurrentFileAppenderPath() As String
        Get
            If m_FileAppender Is Nothing OrElse String.IsNullOrEmpty(m_FileAppender.File) Then
                Return String.Empty
            End If
            Return m_FileAppender.File
        End Get
    End Property

    ''' <summary>
    ''' Tells calling program file debug status
    ''' </summary>
    ''' <returns>TRUE if debug level enabled for file logger; FALSE otherwise</returns>
    ''' <remarks></remarks>
    Public Shared ReadOnly Property FileLogDebugEnabled() As Boolean
        Get
            Return m_FileLogger.IsDebugEnabled
        End Get
    End Property

    Public Shared ReadOnly Property MostRecentErrorMessage() As String
        Get
            Return m_MostRecentErrorMessage
        End Get
    End Property
#End Region

#Region "Methods"

    ''' <summary>
    ''' Write a message to the logging system
    ''' </summary>
    ''' <param name="loggerType">Type of logger to use</param>
    ''' <param name="logLevel">Level of log reporting</param>
    ''' <param name="message">Message to be logged</param>
    ''' <remarks></remarks>
    Public Shared Sub WriteLog(ByVal loggerType As LoggerTypes, ByVal logLevel As LogLevels, ByVal message As String)
        WriteLogWork(loggerType, logLevel, message, Nothing)
    End Sub

    ''' <summary>
    ''' Write a message and exception to the logging system
    ''' </summary>
    ''' <param name="loggerType">Type of logger to use</param>
    ''' <param name="logLevel">Level of log reporting</param>
    ''' <param name="message">Message to be logged</param>
    ''' <param name="ex">Exception to be logged</param>
    ''' <remarks></remarks>
    Public Shared Sub WriteLog(ByVal loggerType As LoggerTypes, ByVal logLevel As LogLevels, ByVal message As String, ByVal ex As Exception)
        WriteLogWork(loggerType, logLevel, message, ex)
    End Sub

    ''' <summary>
    ''' Write a message and possibly an exception to the logging system
    ''' </summary>
    ''' <param name="loggerType">Type of logger to use</param>
    ''' <param name="logLevel">Level of log reporting</param>
    ''' <param name="message">Message to be logged</param>
    ''' <param name="ex">Exception to be logged; nothing if no exception</param>
    ''' <remarks></remarks>
    Private Shared Sub WriteLogWork(ByVal loggerType As LoggerTypes, ByVal logLevel As LogLevels, ByVal message As String, ByVal ex As Exception)
        Dim myLogger As ILog

        ' Establish which logger will be used
        Select Case loggerType
            Case LoggerTypes.LogDb
                myLogger = m_DbLogger
                message = Net.Dns.GetHostName() & ": " & message
            Case LoggerTypes.LogFile
                myLogger = m_FileLogger
                ' Check to determine if a new file should be started
                Dim testFileDate = DateTime.Now.ToString("MM-dd-yyyy")
                If Not String.Equals(testFileDate, m_FileDate) Then
                    m_FileDate = testFileDate
                    ChangeLogFileName()
                End If
            Case LoggerTypes.LogSystem
                myLogger = m_SysLogger
            Case Else
                Throw New Exception("Invalid logger type specified")
        End Select

        ' Send the log message
        Select Case logLevel
            Case LogLevels.DEBUG
                If myLogger.IsDebugEnabled Then
                    If ex Is Nothing Then
                        myLogger.Debug(message)
                    Else
                        myLogger.Debug(message, ex)
                    End If
                End If
            Case LogLevels.ERROR
                If myLogger.IsErrorEnabled Then
                    If ex Is Nothing Then
                        myLogger.Error(message)
                    Else
                        myLogger.Debug(message, ex)
                    End If
                End If
            Case LogLevels.FATAL
                If myLogger.IsFatalEnabled Then
                    If ex Is Nothing Then
                        myLogger.Fatal(message)
                    Else
                        myLogger.Debug(message, ex)
                    End If
                End If
            Case LogLevels.INFO
                If myLogger.IsInfoEnabled Then
                    If ex Is Nothing Then
                        myLogger.Info(message)
                    Else
                        myLogger.Debug(message, ex)
                    End If
                End If
            Case LogLevels.WARN
                If myLogger.IsWarnEnabled Then
                    If ex Is Nothing Then
                        myLogger.Warn(message)
                    Else
                        myLogger.Debug(message, ex)
                    End If
                End If
            Case Else
                Throw New Exception("Invalid log level specified")
        End Select

        If logLevel <= LogLevels.ERROR Then
            m_MostRecentErrorMessage = message
        End If
    End Sub

    ''' <summary>
    ''' Changes the base log file name
    ''' </summary>
    ''' <remarks></remarks>
    Public Shared Sub ChangeLogFileName()
        ChangeLogFileName(m_BaseFileName & "_" & m_FileDate & ".txt")
    End Sub

    ''' <summary>
    ''' Changes the base log file name
    ''' </summary>
    ''' <param name="fileName">Log file base name and path (relative to program folder)</param>
    ''' <remarks>This method is called by the Mage, Ascore, and Multialign plugins</remarks>
    Public Shared Sub ChangeLogFileName(ByVal fileName As String)

        ' Get a list of appenders
        Dim appendList As IEnumerable(Of IAppender) = FindAppenders(LOG_FILE_APPENDER)
        If appendList Is Nothing Then
            WriteLog(LoggerTypes.LogSystem, LogLevels.WARN, "Unable to change file name. No appender found")
            Return
        End If

        For Each selectedAppender As IAppender In appendList
            ' Convert the IAppender object to a FileAppender instance
            Dim AppenderToChange As FileAppender = TryCast(selectedAppender, FileAppender)
            If AppenderToChange Is Nothing Then
                WriteLog(LoggerTypes.LogSystem, LogLevels.ERROR, "Unable to convert appender")
                Return
            End If

            ' Change the file name and activate change
            AppenderToChange.File = fileName
            AppenderToChange.ActivateOptions()
        Next
    End Sub

    ''' <summary>
    ''' Gets the specified appender
    ''' </summary>
    ''' <param name="appenderName">Name of appender to find</param>
    ''' <returns>List(IAppender) objects if found; NOTHING otherwise</returns>
    ''' <remarks></remarks>
    Private Shared Function FindAppenders(ByVal appenderName As String) As IEnumerable(Of IAppender)

        'Get a list of the current loggers
        Dim LoggerList() As ILog = LogManager.GetCurrentLoggers()
        If LoggerList.GetLength(0) < 1 Then Return Nothing

        'Create a List of appenders matching the criteria for each logger
        Dim RetList As New List(Of IAppender)
        For Each testLogger As ILog In LoggerList
            For Each testAppender As IAppender In testLogger.Logger.Repository.GetAppenders()
                If testAppender.Name = appenderName Then RetList.Add(testAppender)
            Next
        Next

        'Return the list of appenders, if any found
        If RetList.Count > 0 Then
            Return RetList
        Else
            Return Nothing
        End If
    End Function

    ''' <summary>
    ''' Sets the file logging level via an integer value (Overloaded)
    ''' </summary>
    ''' <param name="logLevel">Integer corresponding to level (1-5, 5 being most verbose)</param>
    ''' <remarks></remarks>
    Public Shared Sub SetFileLogLevel(ByVal logLevel As Integer)

        Dim logLevelEnumType As Type = GetType(LogLevels)

        'Verify input level is a valid log level
        If Not [Enum].IsDefined(logLevelEnumType, logLevel) Then
            WriteLog(LoggerTypes.LogFile, LogLevels.ERROR, "Invalid value specified for level: " & logLevel.ToString)
            Return
        End If

        'Convert input integer into the associated enum
        Dim logLevelEnum = DirectCast([Enum].Parse(logLevelEnumType, logLevel.ToString), LogLevels)
        SetFileLogLevel(logLevelEnum)

    End Sub

    ''' <summary>
    ''' Sets file logging level based on enumeration (Overloaded)
    ''' </summary>
    ''' <param name="logLevel">LogLevels value defining level (Debug is most verbose)</param>
    ''' <remarks></remarks>
    Public Shared Sub SetFileLogLevel(ByVal logLevel As LogLevels)

        Dim logger = DirectCast(m_FileLogger.Logger, Repository.Hierarchy.Logger)

        Select Case logLevel
            Case LogLevels.DEBUG
                logger.Level = logger.Hierarchy.LevelMap("DEBUG")
            Case LogLevels.ERROR
                logger.Level = logger.Hierarchy.LevelMap("ERROR")
            Case LogLevels.FATAL
                logger.Level = logger.Hierarchy.LevelMap("FATAL")
            Case LogLevels.INFO
                logger.Level = logger.Hierarchy.LevelMap("INFO")
            Case LogLevels.WARN
                logger.Level = logger.Hierarchy.LevelMap("WARN")
        End Select
    End Sub

    ''' <summary>
    ''' Creates a file appender
    ''' </summary>
    ''' <param name="logFileNameBase">Base name for log file</param>
    ''' <returns>A configured file appender</returns>
    Private Shared Function CreateFileAppender(logFileNameBase As String) As FileAppender
        m_FileDate = DateTime.Now.ToString("MM-dd-yyyy")
        m_BaseFileName = logFileNameBase

        Dim layout = New log4net.Layout.PatternLayout()
        layout.ConversionPattern = "%date{MM/dd/yyyy HH:mm:ss}, %message, %level,%newline"

        layout.ActivateOptions()

        Dim returnAppender = New FileAppender()
        With returnAppender
            .Name = LOG_FILE_APPENDER
            .File = m_BaseFileName & "_" & m_FileDate & ".txt"
            .AppendToFile = True
            .Layout = layout
        End With

        returnAppender.ActivateOptions()

        Return returnAppender
    End Function

    ''' <summary>
    ''' Configures the file logger
    ''' </summary>
    ''' <param name="logFileName">Base name for log file</param>
    Public Shared Sub CreateFileLogger(logFileName As String)
        Dim curLogger = DirectCast(m_FileLogger.Logger, log4net.Repository.Hierarchy.Logger)
        m_FileAppender = CreateFileAppender(logFileName)
        curLogger.AddAppender(m_FileAppender)

        ' The analysis manager determines when to log or not log based on internal logic
        ' Set the LogLevel tracked by log4net to DEBUG so that all messages sent to this class are logged
        SetFileLogLevel(LogLevels.DEBUG)
    End Sub

    ''' <summary>
    ''' Configures the Db logger
    ''' </summary>
    ''' <param name="connStr">Database connection string</param>
    ''' <param name="moduleName">Module name used by logger</param>
    ''' <param name="isBeforeMgrControlParams">Should be True when this function is called before parameters are retrieved from the Manager Control DB</param>
    Public Shared Sub CreateDbLogger(connStr As String, moduleName As String, isBeforeMgrControlParams As Boolean)
        Dim curLogger = DirectCast(m_DbLogger.Logger, log4net.Repository.Hierarchy.Logger)
        curLogger.Level = log4net.Core.Level.Info

        If isBeforeMgrControlParams Then
            curLogger.AddAppender(CreateDbAppender(connStr, moduleName, DB_LOGGER_NO_MGR_CONTROL_PARAMS))
        Else
            curLogger.AddAppender(CreateDbAppender(connStr, moduleName, DB_LOGGER_MGR_CONTROL))
        End If

        If m_FileAppender Is Nothing Then
            Return
        End If

        Dim addFileAppender = True
        For Each item In curLogger.Appenders
            If item Is m_FileAppender Then
                addFileAppender = False
                Exit For
            End If
        Next

        If addFileAppender Then
            curLogger.AddAppender(m_FileAppender)
        End If
    End Sub

    ''' <summary>
    ''' Remove the default database logger that was created when the program first started
    ''' </summary>
    Public Shared Sub RemoveDefaultDbLogger()
        Dim curLogger = DirectCast(m_DbLogger.Logger, log4net.Repository.Hierarchy.Logger)

        For Each item In curLogger.Appenders
            If item.Name = DB_LOGGER_NO_MGR_CONTROL_PARAMS Then
                curLogger.RemoveAppender(item)
                item.Close()
                Exit For
            End If
        Next
    End Sub


    ''' <summary>
    ''' Creates a database appender
    ''' </summary>
    ''' <param name="connectionString">Database connection string</param>
    ''' <param name="moduleName">Module name used by logger</param>
    ''' <param name="appenderName">Appender name</param>
    ''' <returns>ADONet database appender</returns>
    Public Shared Function CreateDbAppender(connectionString As String, moduleName As String, appenderName As String) As AdoNetAppender
        Dim returnAppender = New AdoNetAppender()

        With returnAppender
            .BufferSize = 1
            .ConnectionType = "System.Data.SqlClient.SqlConnection, System.Data, Version=1.0.3300.0, Culture=neutral, PublicKeyToken=b77a5c561934e089"
            .ConnectionString = connectionString
            .CommandType = CommandType.StoredProcedure
            .CommandText = "PostLogEntry"
            .Name = appenderName
        End With

        'Type parameter
        Dim typeParam = New AdoNetAppenderParameter()
        With typeParam
            .ParameterName = "@type"
            .DbType = DbType.[String]
            .Size = 50
            .Layout = CreateLayout("%level")
        End With

        returnAppender.AddParameter(typeParam)

        'Message parameter
        Dim msgParam = New AdoNetAppenderParameter()
        With msgParam
            .ParameterName = "@message"
            .DbType = DbType.[String]
            .Size = 4000
            .Layout = CreateLayout("%message")
        End With

        returnAppender.AddParameter(msgParam)

        'PostedBy parameter
        Dim postByParam = New AdoNetAppenderParameter()
        With postByParam
            .ParameterName = "@postedBy"
            .DbType = DbType.[String]
            .Size = 128
            .Layout = CreateLayout(moduleName)
        End With

        returnAppender.AddParameter(postByParam)

        returnAppender.ActivateOptions()

        Return returnAppender
    End Function

    ''' <summary>
    ''' Creates a layout object for a Db appender parameter
    ''' </summary>
    ''' <param name="layoutStr">Name of parameter</param>
    ''' <returns></returns>
    Private Shared Function CreateLayout(layoutStr As String) As log4net.Layout.IRawLayout
        Dim layoutConvert = New log4net.Layout.RawLayoutConverter()
        Dim returnLayout = New log4net.Layout.PatternLayout()
        returnLayout.ConversionPattern = layoutStr

        returnLayout.ActivateOptions()

        Dim retItem = DirectCast(layoutConvert.ConvertFrom(returnLayout), log4net.Layout.IRawLayout)

        If retItem Is Nothing Then
            Throw New ConversionNotSupportedException("Error converting a PatternLayout to IRawLayout")
        End If

        Return retItem

    End Function

#End Region

End Class

