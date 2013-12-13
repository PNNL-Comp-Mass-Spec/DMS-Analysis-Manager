'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2009, Battelle Memorial Institute
' Created 01/01/2009
'
' Last modified 05/14/2009
'						05/15/2009 (DAC) - Modified logging to use Log4Net
'*********************************************************************************************************

Option Strict On

Imports log4net.Appender
Imports log4net

'This assembly attribute tells Log4Net where to find the config file
<Assembly: log4net.Config.XmlConfigurator(ConfigFile:="Logging.config", Watch:=True)> 

Public Class clsLogTools

	'*********************************************************************************************************
	' Class for handling logging via Log4Net
	'*********************************************************************************************************

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
	Private Shared m_MostRecentErrorMessage As String = String.Empty
#End Region

#Region "Properties"
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
	''' Writes a message to the logging system
	''' </summary>
	''' <param name="LoggerType">Type of logger to use</param>
	''' <param name="LogLevel">Level of log reporting</param>
	''' <param name="InpMsg">Message to be logged</param>
	''' <remarks></remarks>
	Public Shared Sub WriteLog(ByVal LoggerType As LoggerTypes, ByVal LogLevel As LogLevels, ByVal InpMsg As String)

		Dim MyLogger As ILog

		'Establish which logger will be used
		Select Case LoggerType
			Case LoggerTypes.LogDb
				' Note that the Logging.config should have the DbLogger logging to both the database and the rolling file
				MyLogger = m_DbLogger
			Case LoggerTypes.LogFile
				MyLogger = m_FileLogger
			Case LoggerTypes.LogSystem
				MyLogger = m_SysLogger
			Case Else
				Throw New Exception("Invalid logger type specified")
		End Select

		'Send the log message
		Select Case LogLevel
			Case LogLevels.DEBUG
				If MyLogger.IsDebugEnabled Then MyLogger.Debug(InpMsg)
			Case LogLevels.ERROR
				If MyLogger.IsErrorEnabled Then MyLogger.Error(InpMsg)
			Case LogLevels.FATAL
				If MyLogger.IsFatalEnabled Then MyLogger.Fatal(InpMsg)
			Case LogLevels.INFO
				If MyLogger.IsInfoEnabled Then MyLogger.Info(InpMsg)
			Case LogLevels.WARN
				If MyLogger.IsWarnEnabled Then MyLogger.Warn(InpMsg)
			Case Else
				Throw New Exception("Invalid log level specified")
		End Select

		If LogLevel <= LogLevels.ERROR Then
			m_MostRecentErrorMessage = InpMsg
		End If
	End Sub

	''' <summary>
	''' Overload to write a message and exception to the logging system
	''' </summary>
	''' <param name="LoggerType">Type of logger to use</param>
	''' <param name="LogLevel">Level of log reporting</param>
	''' <param name="InpMsg">Message to be logged</param>
	''' <param name="Ex">Exception to be logged</param>
	''' <remarks></remarks>
	Public Shared Sub WriteLog(ByVal LoggerType As LoggerTypes, ByVal LogLevel As LogLevels, ByVal InpMsg As String, _
	 ByVal Ex As Exception)

		Dim MyLogger As ILog

		'Establish which logger will be used
		Select Case LoggerType
			Case LoggerTypes.LogDb
				MyLogger = m_DbLogger
			Case LoggerTypes.LogFile
				MyLogger = m_FileLogger
			Case LoggerTypes.LogSystem
				MyLogger = m_SysLogger
			Case Else
				Throw New Exception("Invalid logger type specified")
		End Select

		'Send the log message
		Select Case LogLevel
			Case LogLevels.DEBUG
				If MyLogger.IsDebugEnabled Then MyLogger.Debug(InpMsg, Ex)
			Case LogLevels.ERROR
				If MyLogger.IsErrorEnabled Then MyLogger.Error(InpMsg, Ex)
			Case LogLevels.FATAL
				If MyLogger.IsFatalEnabled Then MyLogger.Fatal(InpMsg, Ex)
			Case LogLevels.INFO
				If MyLogger.IsInfoEnabled Then MyLogger.Info(InpMsg, Ex)
			Case LogLevels.WARN
				If MyLogger.IsWarnEnabled Then MyLogger.Warn(InpMsg, Ex)
			Case Else
				Throw New Exception("Invalid log level specified")
		End Select
	End Sub

	''' <summary>
	''' Changes the base log file name
	''' </summary>
	''' <param name="FileName">Log file base name and path (relative to program folder)</param>
	''' <remarks></remarks>
	Public Shared Sub ChangeLogFileName(ByVal FileName As String)

		'Get a list of appenders
		Dim AppendList As IEnumerable(Of IAppender) = FindAppenders("RollingFileAppender")
		If AppendList Is Nothing Then
			WriteLog(LoggerTypes.LogSystem, LogLevels.WARN, "Unable to change file name. No appender found")
			Return
		End If

		For Each SelectedAppender As IAppender In AppendList
			'Convert the IAppender object to a RollingFileAppender
			Dim AppenderToChange As RollingFileAppender = TryCast(SelectedAppender, RollingFileAppender)
			If AppenderToChange Is Nothing Then
				WriteLog(LoggerTypes.LogSystem, LogLevels.ERROR, "Unable to convert appender")
				Return
			End If
			'Change the file name and activate change
			AppenderToChange.File = FileName
			AppenderToChange.ActivateOptions()
		Next
	End Sub

	''' <summary>
	''' Gets the specified appender
	''' </summary>
	''' <param name="AppendName">Name of appender to find</param>
	''' <returns>List(IAppender) objects if found; NOTHING otherwise</returns>
	''' <remarks></remarks>
	Private Shared Function FindAppenders(ByVal AppendName As String) As IEnumerable(Of IAppender)

		'Get a list of the current loggers
		Dim LoggerList() As ILog = LogManager.GetCurrentLoggers()
		If LoggerList.GetLength(0) < 1 Then Return Nothing

		'Create a List of appenders matching the criteria for each logger
		Dim RetList As New List(Of IAppender)
		For Each TestLogger As ILog In LoggerList
			For Each TestAppender As IAppender In TestLogger.Logger.Repository.GetAppenders()
				If TestAppender.Name = AppendName Then RetList.Add(TestAppender)
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
	''' <param name="InpLevel">Integer corresponding to level (1-5, 5 being most verbose</param>
	''' <remarks></remarks>
	Public Shared Sub SetFileLogLevel(ByVal InpLevel As Integer)

		Dim LogLevelEnumType As Type = GetType(LogLevels)

		'Verify input level is a valid log level
		If Not [Enum].IsDefined(LogLevelEnumType, InpLevel) Then
			WriteLog(LoggerTypes.LogFile, LogLevels.ERROR, "Invalid value specified for level: " & InpLevel.ToString)
			Return
		End If

		'Convert input integer into the associated enum
		Dim Lvl As LogLevels = DirectCast([Enum].Parse(LogLevelEnumType, InpLevel.ToString), LogLevels)
		SetFileLogLevel(Lvl)

	End Sub
	'%date [%thread] %-5level %logger [%property{NDC}] - %message%newline"

	''' <summary>
	''' Sets file logging level based on enumeration (Overloaded)
	''' </summary>
	''' <param name="InpLevel">LogLevels value defining level (Debug is most verbose)</param>
	''' <remarks></remarks>
	Public Shared Sub SetFileLogLevel(ByVal InpLevel As LogLevels)

		Dim LogRepo As Repository.Hierarchy.Logger = DirectCast(m_FileLogger.Logger, Repository.Hierarchy.Logger)

		Select Case InpLevel
			Case LogLevels.DEBUG
				LogRepo.Level = LogRepo.Hierarchy.LevelMap("DEBUG")
			Case LogLevels.ERROR
				LogRepo.Level = LogRepo.Hierarchy.LevelMap("ERROR")
			Case LogLevels.FATAL
				LogRepo.Level = LogRepo.Hierarchy.LevelMap("FATAL")
			Case LogLevels.INFO
				LogRepo.Level = LogRepo.Hierarchy.LevelMap("INFO")
			Case LogLevels.WARN
				LogRepo.Level = LogRepo.Hierarchy.LevelMap("WARN")
		End Select
	End Sub
#End Region

End Class

