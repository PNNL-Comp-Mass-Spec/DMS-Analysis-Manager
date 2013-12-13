'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 06/11/2009 JDS - Added logging using log4net and status monitoring using activeMQ
'*********************************************************************************************************

Option Strict On

Imports System.IO
Imports System.Xml

Public Class clsStatusFile
	Implements IStatusFile

	'*********************************************************************************************************
	'Provides tools for creating and updating an analysis status file
	' Additional functionality:
	'  1) Can log memory usage stats to a file using clsMemoryUsageLogger
	'  2) Looks for the presence of file "AbortProcessingNow.txt"; if found, it sets AbortProcessingNow to true 
	'	 and renames the file to "AbortProcessingNow.txt.Done"
	'  3) Posts status messages to the DMS broker DB at the specified interval
	'
	'*********************************************************************************************************

#Region "Module variables"
	Public Const ABORT_PROCESSING_NOW_FILENAME As String = "AbortProcessingNow.txt"

	'Status file name and location
	Private m_FileNamePath As String = ""

	'Manager name
	Private m_MgrName As String = ""

	'Status value
	Private m_MgrStatus As IStatusFile.EnumMgrStatus = IStatusFile.EnumMgrStatus.STOPPED

	'CPU utilization
	Private m_CpuUtilization As Integer = 0

	' Free Memory
	Private m_FreeMemoryMB As Single = 0

	'Analysis Tool (aka step tool)
	Private m_Tool As String = ""

	'Task status
	Private m_TaskStatus As IStatusFile.EnumTaskStatus = IStatusFile.EnumTaskStatus.NO_TASK

	'Task start time
	Private m_TaskStartTime As Date = DateTime.UtcNow

	'Progess (in percent)
	Private m_Progress As Single = 0

	'Current operation (freeform string)
	Private m_CurrentOperation As String = ""

	'Task status detail
	Private m_TaskStatusDetail As IStatusFile.EnumTaskStatusDetail = IStatusFile.EnumTaskStatusDetail.NO_TASK

	'Job number
	Private m_JobNumber As Integer

	'Job step
	Private m_JobStep As Integer = 0

	'Dataset name
	Private m_Dataset As String = ""

	'Most recent job info
	Private m_MostRecentJobInfo As String = ""

	'Number of spectrum files created
	Private m_SpectrumCount As Integer = 0

	Private m_LogToMessageQueue As Boolean = False
	Private m_MessageQueueURI As String
	Private m_MessageQueueTopic As String

	'Flag to indicate that the ABORT_PROCESSING_NOW_FILENAME file was detected
	Private m_AbortProcessingNow As Boolean = False

	Private m_MostRecentLogMessage As String

	Const MAX_ERROR_MESSAGE_COUNT_TO_CACHE As Integer = 10
	Private m_RecentErrorMessageCount As Integer

	' ReSharper disable once FieldCanBeMadeReadOnly.Local
	Private m_RecentErrorMessages(MAX_ERROR_MESSAGE_COUNT_TO_CACHE - 1) As String

	' The following provides access to the master logger
	Protected m_DebugLevel As Integer

	' Used to log the memory usage to a status file
	Private m_MemoryUsageLogger As clsMemoryUsageLogger

	' Used to log messages to the broker DB
	Private m_BrokerDBLogger As clsDBStatusLogger

	Private m_MessageQueueLogger As clsMessageQueueLogger
	Private m_MessageSender As clsMessageSender
	Private m_QueueLogger As clsMessageQueueLogger

	Private mCPUUsagePerformanceCounter As PerformanceCounter
	Private mFreeMemoryPerformanceCounter As PerformanceCounter

#End Region

#Region "Properties"
	Public Property FileNamePath() As String Implements IStatusFile.FileNamePath
		Get
			Return m_FileNamePath
		End Get
		Set(ByVal value As String)
			m_FileNamePath = value
		End Set
	End Property

	Public Property MgrName() As String Implements IStatusFile.MgrName
		Get
			Return m_MgrName
		End Get
		Set(ByVal Value As String)
			m_MgrName = Value
		End Set
	End Property

	Public Property MgrStatus() As IStatusFile.EnumMgrStatus Implements IStatusFile.MgrStatus
		Get
			Return m_MgrStatus
		End Get
		Set(ByVal Value As IStatusFile.EnumMgrStatus)
			m_MgrStatus = Value
		End Set
	End Property

	Public Property CpuUtilization() As Integer Implements IStatusFile.CpuUtilization
		Get
			Return m_CpuUtilization
		End Get
		Set(ByVal value As Integer)
			m_CpuUtilization = value
		End Set
	End Property

	Public ReadOnly Property FreeMemoryMB() As Single
		Get
			Return m_FreeMemoryMB
		End Get
	End Property

	Public Property Tool() As String Implements IStatusFile.Tool
		Get
			Return m_Tool
		End Get
		Set(ByVal Value As String)
			m_Tool = Value
		End Set
	End Property

	Public Property TaskStatus() As IStatusFile.EnumTaskStatus Implements IStatusFile.TaskStatus
		Get
			Return m_TaskStatus
		End Get
		Set(ByVal value As IStatusFile.EnumTaskStatus)
			m_TaskStatus = value
		End Set
	End Property

	Public Property TaskStartTime() As DateTime
		Get
			Return m_TaskStartTime
		End Get
		Set(ByVal value As DateTime)
			m_TaskStartTime = value
		End Set
	End Property

	Public Property Progress() As Single Implements IStatusFile.Progress
		Get
			Return m_Progress
		End Get
		Set(ByVal Value As Single)
			m_Progress = Value
		End Set
	End Property

	Public Property CurrentOperation() As String Implements IStatusFile.CurrentOperation
		Get
			Return m_CurrentOperation
		End Get
		Set(ByVal value As String)
			m_CurrentOperation = value
		End Set
	End Property

	Public Property TaskStatusDetail() As IStatusFile.EnumTaskStatusDetail Implements IStatusFile.TaskStatusDetail
		Get
			Return m_TaskStatusDetail
		End Get
		Set(ByVal value As IStatusFile.EnumTaskStatusDetail)
			m_TaskStatusDetail = value
		End Set
	End Property

	Public Property JobNumber() As Integer Implements IStatusFile.JobNumber
		Get
			Return m_JobNumber
		End Get
		Set(ByVal Value As Integer)
			m_JobNumber = Value
		End Set
	End Property

	Public Property JobStep() As Integer Implements IStatusFile.JobStep
		Get
			Return m_JobStep
		End Get
		Set(ByVal value As Integer)
			m_JobStep = value
		End Set
	End Property

	Public Property Dataset() As String Implements IStatusFile.Dataset
		Get
			Return m_Dataset
		End Get
		Set(ByVal Value As String)
			m_Dataset = Value
		End Set
	End Property

	Public Property MostRecentJobInfo() As String Implements IStatusFile.MostRecentJobInfo
		Get
			Return m_MostRecentJobInfo
		End Get
		Set(ByVal value As String)
			m_MostRecentJobInfo = value
		End Set
	End Property

	Public Property SpectrumCount() As Integer Implements IStatusFile.SpectrumCount
		Get
			Return m_SpectrumCount
		End Get
		Set(ByVal value As Integer)
			m_SpectrumCount = value
		End Set
	End Property

	Public Property MessageQueueURI() As String Implements IStatusFile.MessageQueueURI
		Get
			Return m_MessageQueueURI
		End Get
		Set(ByVal value As String)
			m_MessageQueueURI = value
		End Set
	End Property

	Public Property MessageQueueTopic() As String Implements IStatusFile.MessageQueueTopic
		Get
			Return m_MessageQueueTopic
		End Get
		Set(ByVal value As String)
			m_MessageQueueTopic = value
		End Set
	End Property

	Public Property LogToMsgQueue() As Boolean Implements IStatusFile.LogToMsgQueue
		Get
			Return m_LogToMessageQueue
		End Get
		Set(ByVal value As Boolean)
			m_LogToMessageQueue = value
		End Set
	End Property

	Public ReadOnly Property AbortProcessingNow() As Boolean
		Get
			Return m_AbortProcessingNow
		End Get
	End Property
#End Region

#Region "Methods"

	''' <summary>
	''' Constructor
	''' </summary>
	''' <param name="FileLocation">Full path to status file</param>
	''' <remarks></remarks>
	Public Sub New(ByVal FileLocation As String, ByVal debugLevel As Integer)
		m_FileNamePath = FileLocation
		m_TaskStartTime = DateTime.UtcNow
		m_DebugLevel = debugLevel

		ClearCachedInfo()

		InitializePerformanceCounters()
	End Sub

	''' <summary>
	''' Configure the memory logging settings
	''' </summary>
	''' <param name="LogMemoryUsage"></param>
	''' <param name="MinimumMemoryUsageLogIntervalMinutes"></param>
	''' <param name="MemoryUsageLogFolderPath"></param>
	''' <remarks></remarks>
	Public Sub ConfigureMemoryLogging(ByVal LogMemoryUsage As Boolean, ByVal MinimumMemoryUsageLogIntervalMinutes As Single, ByVal MemoryUsageLogFolderPath As String)
		If LogMemoryUsage Then
			If m_MemoryUsageLogger Is Nothing Then
				m_MemoryUsageLogger = New clsMemoryUsageLogger(MemoryUsageLogFolderPath, MinimumMemoryUsageLogIntervalMinutes)
			Else
				m_MemoryUsageLogger.MinimumLogIntervalMinutes = MinimumMemoryUsageLogIntervalMinutes
			End If
		Else
			If Not m_MemoryUsageLogger Is Nothing Then
				' Stop logging memory usage
				m_MemoryUsageLogger = Nothing
			End If
		End If
	End Sub

	''' <summary>
	''' Configure the Broker DB logging settings
	''' </summary>
	''' <param name="LogStatusToBrokerDB"></param>
	''' <param name="BrokerDBConnectionString"></param>
	''' <param name="BrokerDBStatusUpdateIntervalMinutes"></param>
	''' <remarks></remarks>
	Public Sub ConfigureBrokerDBLogging(ByVal LogStatusToBrokerDB As Boolean, ByVal BrokerDBConnectionString As String, ByVal BrokerDBStatusUpdateIntervalMinutes As Single)
		If LogStatusToBrokerDB Then
			If m_BrokerDBLogger Is Nothing Then
				m_BrokerDBLogger = New clsDBStatusLogger(BrokerDBConnectionString, BrokerDBStatusUpdateIntervalMinutes)
			Else
				m_BrokerDBLogger.DBStatusUpdateIntervalMinutes = BrokerDBStatusUpdateIntervalMinutes
			End If
		Else
			If Not m_BrokerDBLogger Is Nothing Then
				' Stop logging to the broker
				m_BrokerDBLogger = Nothing
			End If
		End If
	End Sub

	''' <summary>
	''' Configure the Message Queue logging settings
	''' </summary>
	''' <param name="LogStatusToMessageQueue"></param>
	''' <param name="MsgQueueURI"></param>
	''' <param name="MessageQueueTopicMgrStatus"></param>
	''' <param name="ClientName"></param>
	''' <remarks></remarks>
	Public Sub ConfigureMessageQueueLogging(ByVal LogStatusToMessageQueue As Boolean, ByVal MsgQueueURI As String, ByVal MessageQueueTopicMgrStatus As String, ByVal ClientName As String)
		m_LogToMessageQueue = LogStatusToMessageQueue
		m_MessageQueueURI = MsgQueueURI
		m_MessageQueueTopic = MessageQueueTopicMgrStatus

		' m_MessageQueueClientName = ClientName

		If Not m_LogToMessageQueue And Not m_MessageQueueLogger Is Nothing Then
			' Stop logging to the message queue
			m_MessageQueueLogger = Nothing
		End If
	End Sub

	''' <summary>
	''' Looks for file "AbortProcessingNow.txt"
	''' If found, sets property AbortProcessingNow to True
	''' </summary>
	''' <remarks></remarks>
	Public Sub CheckForAbortProcessingFile()
		Dim strPathToCheck As String
		Dim strNewPath As String

		Try
			strPathToCheck = Path.GetDirectoryName(m_FileNamePath)
			strPathToCheck = Path.Combine(strPathToCheck, ABORT_PROCESSING_NOW_FILENAME)

			If File.Exists(strPathToCheck) Then
				m_AbortProcessingNow = True

				strNewPath = strPathToCheck & ".done"

				File.Delete(strNewPath)
				File.Move(strPathToCheck, strNewPath)

			End If
		Catch ex As Exception
			' Ignore errors here
		End Try

	End Sub

	''' <summary>
	''' Clears the cached information about dataset, job, progress, etc.
	''' </summary>
	''' <remarks></remarks>
	Private Sub ClearCachedInfo()
		m_Progress = 0
		m_SpectrumCount = 0
		m_Dataset = ""
		m_JobNumber = 0
		m_JobStep = 0
		m_Tool = ""

		' Only clear the recent job info if the variable is Nothing
		If m_MostRecentJobInfo Is Nothing Then
			m_MostRecentJobInfo = String.Empty
		End If

		m_MostRecentLogMessage = String.Empty

		m_RecentErrorMessageCount = 0
		m_RecentErrorMessages(0) = String.Empty
	End Sub

	''' <summary>
	''' Converts the job status enum to a string value
	''' </summary>
	''' <param name="StatusEnum">An IStatusFile.JobStatus object</param>
	''' <returns>String representation of input object</returns>
	''' <remarks></remarks>
	Private Function ConvertTaskStatusToString(ByVal StatusEnum As IStatusFile.EnumTaskStatus) As String

		'Converts a status enum to a string
		Select Case StatusEnum
			Case IStatusFile.EnumTaskStatus.CLOSING
				Return "Closing"
			Case IStatusFile.EnumTaskStatus.NO_TASK
				Return "No Task"
			Case IStatusFile.EnumTaskStatus.RUNNING
				Return "Running"
			Case IStatusFile.EnumTaskStatus.REQUESTING
				Return "Requesting"
			Case IStatusFile.EnumTaskStatus.STOPPED
				Return "Stopped"
			Case IStatusFile.EnumTaskStatus.FAILED
				Return "Failed"
			Case Else
				'Should never get here
				Return "Unknown Task Status"
		End Select

	End Function

	''' <summary>
	''' Converts the job status enum to a string value
	''' </summary>
	''' <param name="StatusEnum">An IStatusFile.JobStatus object</param>
	''' <returns>String representation of input object</returns>
	''' <remarks></remarks>
	Private Function ConvertMgrStatusToString(ByVal StatusEnum As IStatusFile.EnumMgrStatus) As String

		'Converts a status enum to a string
		Select Case StatusEnum
			Case IStatusFile.EnumMgrStatus.DISABLED_LOCAL
				Return "Disabled Local"
			Case IStatusFile.EnumMgrStatus.DISABLED_MC
				Return "Disabled MC"
			Case IStatusFile.EnumMgrStatus.RUNNING
				Return "Running"
			Case IStatusFile.EnumMgrStatus.STOPPED
				Return "Stopped"
			Case IStatusFile.EnumMgrStatus.STOPPED_ERROR
				Return "Stopped Error"
			Case Else
				'Should never get here
				Return "Unknown Mgr Status"
		End Select

	End Function

	''' <summary>
	''' Converts the job status enum to a string value
	''' </summary>
	''' <param name="StatusEnum">An IStatusFile.JobStatus object</param>
	''' <returns>String representation of input object</returns>
	''' <remarks></remarks>
	Private Function ConvertTaskStatusDetailToString(ByVal StatusEnum As IStatusFile.EnumTaskStatusDetail) As String

		'Converts a status enum to a string
		Select Case StatusEnum
			Case IStatusFile.EnumTaskStatusDetail.DELIVERING_RESULTS
				Return "Delivering Results"
			Case IStatusFile.EnumTaskStatusDetail.NO_TASK
				Return "No Task"
			Case IStatusFile.EnumTaskStatusDetail.PACKAGING_RESULTS
				Return "Packaging Results"
			Case IStatusFile.EnumTaskStatusDetail.RETRIEVING_RESOURCES
				Return "Retrieving Resources"
			Case IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL
				Return "Running Tool"
			Case IStatusFile.EnumTaskStatusDetail.CLOSING
				Return "Closing"
			Case Else
				'Should never get here
				Return "Unknown Task Status Detail"
		End Select

	End Function

	Private Sub InitializePerformanceCounters()
		Dim blnVirtualMachineOnPIC As Boolean = clsGlobal.UsingVirtualMachineOnPIC()

		Try
			mCPUUsagePerformanceCounter = New PerformanceCounter("Processor", "% Processor Time", "_Total")
			mCPUUsagePerformanceCounter.ReadOnly = True
		Catch ex As Exception
			' To avoid seeing this in the logs continually, we will only post this log message between 12 am and 12:30 am
			If Not blnVirtualMachineOnPIC AndAlso DateTime.Now().Hour = 0 AndAlso DateTime.Now().Minute <= 30 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error instantiating the Processor.[% Processor Time] performance counter (this message is only logged between 12 am and 12:30 am): " & ex.Message)
			End If
		End Try

		Try
			mFreeMemoryPerformanceCounter = New PerformanceCounter("Memory", "Available MBytes")
			mFreeMemoryPerformanceCounter.ReadOnly = True
		Catch ex As Exception
			' To avoid seeing this in the logs continually, we will only post this log message between 12 am and 12:30 am
			' A possible fix for this is to add the user who is running this process to the "Performance Monitor Users" group in "Local Users and Groups" on the machine showing this error.  
			' Alternatively, add the user to the "Administrators" group.
			' In either case, you will need to reboot the computer for the change to take effect
			If Not blnVirtualMachineOnPIC AndAlso DateTime.Now().Hour = 0 AndAlso DateTime.Now().Minute <= 30 Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error instantiating the Memory.[Available MBytes] performance counter (this message is only logged between 12 am and 12:30 am): " & ex.Message)
			End If

		End Try
	End Sub

	''' <summary>
	''' Returns the CPU usage
	''' </summary>
	''' <returns>Value between 0 and 100</returns>
	''' <remarks>This is CPU usage for all running applications, not just this application</remarks>
	Private Function GetCPUUtilization() As Single
		Dim sngCPUUtilization As Single = 0

		Try
			If Not mCPUUsagePerformanceCounter Is Nothing Then
				sngCPUUtilization = mCPUUsagePerformanceCounter.NextValue()
			End If
		Catch ex As Exception
			' Ignore errors here
		End Try

		Return sngCPUUtilization

	End Function

	''' <summary>
	''' Returns the amount of free memory
	''' </summary>
	''' <returns>Amount of free memory, in MB</returns>
	''' <remarks></remarks>
	Public Function GetFreeMemoryMB() As Single
		Dim sngFreeMemory As Single = 0

		Try
			If Not mFreeMemoryPerformanceCounter Is Nothing Then
				sngFreeMemory = mFreeMemoryPerformanceCounter.NextValue()
			End If

			If sngFreeMemory < Single.Epsilon Then
				sngFreeMemory = CSng(New Devices.ComputerInfo().AvailablePhysicalMemory / 1024.0 / 1024.0)
			End If

		Catch ex As Exception
			' Ignore errors here
		End Try

		Return sngFreeMemory

	End Function

	Protected Sub LogStatusToMessageQueue(ByVal strStatusXML As String)

		Const MINIMUM_LOG_FAILURE_INTERVAL_MINUTES As Single = 10
		Static dtLastFailureTime As DateTime = DateTime.UtcNow.Subtract(New TimeSpan(1, 0, 0))

		Try
			If m_MessageSender Is Nothing Then

				If m_DebugLevel >= 5 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Initializing message queue with URI '" & m_MessageQueueURI & "' and Topic '" & m_MessageQueueTopic & "'")
				End If

				m_MessageSender = New clsMessageSender(m_MessageQueueURI, m_MessageQueueTopic, m_MgrName)

				' message queue logger sets up local message buffering (so calls to log don't block)
				' and uses message sender (as a delegate) to actually send off the messages
				m_QueueLogger = New clsMessageQueueLogger()
				AddHandler m_QueueLogger.Sender, New MessageSenderDelegate(AddressOf m_MessageSender.SendMessage)

				If m_DebugLevel >= 3 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Message queue initialized with URI '" & m_MessageQueueURI & "'; posting to Topic '" & m_MessageQueueTopic & "'")
				End If

			End If

			If Not m_QueueLogger Is Nothing Then
				m_QueueLogger.LogStatusMessage(strStatusXML)
			End If

		Catch ex As Exception
			If DateTime.UtcNow.Subtract(dtLastFailureTime).TotalMinutes >= MINIMUM_LOG_FAILURE_INTERVAL_MINUTES Then
				dtLastFailureTime = DateTime.UtcNow
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in clsStatusFile.LogStatusToMessageQueue (B): " & ex.Message)
			End If

		End Try


	End Sub

	Protected Sub LogStatusToBrokerDatabase(ByVal ForceLogToBrokerDB As Boolean)

		Dim intIndex As Integer

		Dim udtStatusInfo As clsDBStatusLogger.udtStatusInfoType
		With udtStatusInfo
			.MgrName = m_MgrName
			.MgrStatus = m_MgrStatus
			.LastUpdate = DateTime.UtcNow()
			.LastStartTime = m_TaskStartTime
			.CPUUtilization = m_CpuUtilization
			.FreeMemoryMB = m_FreeMemoryMB

			If m_RecentErrorMessageCount = 0 Then
				.MostRecentErrorMessage = String.Empty
			Else
				.MostRecentErrorMessage = m_RecentErrorMessages(0)
				If m_RecentErrorMessageCount > 1 Then
					' Append the next two error messages
					For intIndex = 1 To m_RecentErrorMessageCount - 1
						.MostRecentErrorMessage &= ControlChars.NewLine & m_RecentErrorMessages(intIndex)
						If intIndex >= 2 Then Exit For
					Next
				End If
			End If

			.Task.Tool = m_Tool
			.Task.Status = m_TaskStatus
			.Task.DurationHours = GetRunTime()
			.Task.Progress = m_Progress
			.Task.CurrentOperation = m_CurrentOperation

			.Task.TaskDetails.Status = m_TaskStatusDetail
			.Task.TaskDetails.Job = m_JobNumber
			.Task.TaskDetails.JobStep = m_JobStep
			.Task.TaskDetails.Dataset = m_Dataset
			.Task.TaskDetails.MostRecentLogMessage = m_MostRecentLogMessage
			.Task.TaskDetails.MostRecentJobInfo = m_MostRecentJobInfo
			.Task.TaskDetails.SpectrumCount = m_SpectrumCount

		End With

		m_BrokerDBLogger.LogStatus(udtStatusInfo, ForceLogToBrokerDB)
	End Sub

	Protected Sub StoreRecentJobInfo(ByVal JobInfo As String)
		If Not JobInfo Is Nothing AndAlso JobInfo.Length > 0 Then
			m_MostRecentJobInfo = JobInfo
		End If
	End Sub

	Protected Sub StoreNewErrorMessage(ByVal strErrorMessage As String, ByVal blnClearExistingMessages As Boolean)
		Dim intIndex As Integer

		If blnClearExistingMessages Then
			If strErrorMessage Is Nothing Then
				m_RecentErrorMessageCount = 0
			Else
				m_RecentErrorMessageCount = 1
				m_RecentErrorMessages(0) = strErrorMessage
			End If
		Else
			If Not strErrorMessage Is Nothing AndAlso strErrorMessage.Length > 0 Then
				If m_RecentErrorMessageCount < MAX_ERROR_MESSAGE_COUNT_TO_CACHE Then
					m_RecentErrorMessageCount += 1
				End If

				' Shift each of the entries by one
				For intIndex = m_RecentErrorMessageCount To 1 Step -1
					m_RecentErrorMessages(intIndex) = m_RecentErrorMessages(intIndex - 1)
				Next intIndex

				' Store the new message
				m_RecentErrorMessages(0) = strErrorMessage
			End If
		End If

	End Sub

	''' <summary>
	''' Copies messages from RecentErrorMessages() to m_RecentErrorMessages(); ignores messages that are Nothing or blank
	''' </summary>
	''' <param name="RecentErrorMessages"></param>
	''' <remarks></remarks>
	Protected Sub StoreRecentErrorMessages(ByRef RecentErrorMessages() As String)
		Dim intIndex As Integer

		If RecentErrorMessages Is Nothing Then
			StoreNewErrorMessage("", True)
		Else
			m_RecentErrorMessageCount = 0

			For intIndex = 0 To RecentErrorMessages.Length - 1
				If Not RecentErrorMessages(intIndex) Is Nothing AndAlso RecentErrorMessages(intIndex).Length > 0 Then
					m_RecentErrorMessages(m_RecentErrorMessageCount) = RecentErrorMessages(intIndex)
					m_RecentErrorMessageCount += 1
				End If
			Next

			If m_RecentErrorMessageCount = 0 Then
				' No valid messages were found in RecentErrorMessages()
				' Call StoreNewErrorMessage to clear the stored error messages
				StoreNewErrorMessage("", True)
			End If
		End If
	End Sub

	''' <summary>
	''' Writes the status file
	''' </summary>
	''' <remarks></remarks>
	Public Sub WriteStatusFile() Implements IStatusFile.WriteStatusFile
		WriteStatusFile(False)
	End Sub

	''' <summary>
	''' Updates the status in various locations, including on disk and with the message broker and/or broker DB
	''' </summary>
	''' <param name="ForceLogToBrokerDB">If true, then will force m_BrokerDBLogger to report the manager status to the database</param>
	''' <remarks></remarks>
	Public Sub WriteStatusFile(ByVal ForceLogToBrokerDB As Boolean) Implements IStatusFile.WriteStatusFile

		'Writes a status file for external monitor to read
		Dim objMemoryStream As MemoryStream
		Dim srMemoryStreamReader As StreamReader

		Dim strXMLText As String = String.Empty

		Dim dtLastUpdate As DateTime
		Dim sngRunTimeHours As Single

		Try
			dtLastUpdate = DateTime.UtcNow()
			sngRunTimeHours = GetRunTime()

			m_CpuUtilization = CInt(GetCPUUtilization())
			m_FreeMemoryMB = GetFreeMemoryMB()
		Catch ex As Exception

		End Try

		'Set up the XML writer
		Try
			' Create a new memory stream in which to write the XML
			objMemoryStream = New MemoryStream
			Using XWriter = New XmlTextWriter(objMemoryStream, Text.Encoding.UTF8)

				XWriter.Formatting = Formatting.Indented
				XWriter.Indentation = 2

				'Create the XML document in memory
				XWriter.WriteStartDocument(True)
				XWriter.WriteComment("Analysis manager job status")

				'General job information
				'Root level element
				XWriter.WriteStartElement("Root")	 ' Root
				XWriter.WriteStartElement("Manager")  ' Manager
				XWriter.WriteElementString("MgrName", m_MgrName)
				XWriter.WriteElementString("MgrStatus", ConvertMgrStatusToString(m_MgrStatus))
				XWriter.WriteElementString("LastUpdate", dtLastUpdate.ToLocalTime().ToString())
				XWriter.WriteElementString("LastStartTime", m_TaskStartTime.ToLocalTime().ToString())
				XWriter.WriteElementString("CPUUtilization", m_CpuUtilization.ToString("##0.0"))
				XWriter.WriteElementString("FreeMemoryMB", m_FreeMemoryMB.ToString("##0.0"))
				XWriter.WriteStartElement("RecentErrorMessages")
				If m_RecentErrorMessageCount = 0 Then
					XWriter.WriteElementString("ErrMsg", String.Empty)
				Else
					For intErrorMsgIndex As Integer = 0 To m_RecentErrorMessageCount - 1
						XWriter.WriteElementString("ErrMsg", m_RecentErrorMessages(intErrorMsgIndex))
					Next
				End If
				XWriter.WriteEndElement()				' RecentErrorMessages
				XWriter.WriteEndElement()				' Manager

				XWriter.WriteStartElement("Task")		' Task
				XWriter.WriteElementString("Tool", m_Tool)
				XWriter.WriteElementString("Status", ConvertTaskStatusToString(m_TaskStatus))
				XWriter.WriteElementString("Duration", sngRunTimeHours.ToString("0.00"))
				XWriter.WriteElementString("DurationMinutes", (sngRunTimeHours * 60).ToString("0.0"))
				XWriter.WriteElementString("Progress", m_Progress.ToString("##0.00"))
				XWriter.WriteElementString("CurrentOperation", m_CurrentOperation)

				XWriter.WriteStartElement("TaskDetails") 'TaskDetails
				XWriter.WriteElementString("Status", ConvertTaskStatusDetailToString(m_TaskStatusDetail))
				XWriter.WriteElementString("Job", CStr(m_JobNumber))
				XWriter.WriteElementString("Step", CStr(m_JobStep))
				XWriter.WriteElementString("Dataset", m_Dataset)
				XWriter.WriteElementString("MostRecentLogMessage", m_MostRecentLogMessage)
				XWriter.WriteElementString("MostRecentJobInfo", m_MostRecentJobInfo)
				XWriter.WriteElementString("SpectrumCount", m_SpectrumCount.ToString)
				XWriter.WriteEndElement()				' TaskDetails
				XWriter.WriteEndElement()				' Task
				XWriter.WriteEndElement()				' Root

				'Close out the XML document (but do not close XWriter yet)
				XWriter.WriteEndDocument()
				XWriter.Flush()

				' Now use a StreamReader to copy the XML text to a string variable
				objMemoryStream.Seek(0, SeekOrigin.Begin)
				srMemoryStreamReader = New StreamReader(objMemoryStream)
				strXMLText = srMemoryStreamReader.ReadToEnd

				srMemoryStreamReader.Close()
				objMemoryStream.Close()

				' Since strXMLText now contains the XML, we can now safely close XWriter
			End Using

			WriteStatusFileToDisk(strXMLText)

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error generating status info: " & ex.Message)
		End Try

		CheckForAbortProcessingFile()

		If m_LogToMessageQueue Then
			' Send the XML text to a message queue
			LogStatusToMessageQueue(strXMLText)
		End If

		If Not m_MemoryUsageLogger Is Nothing Then
			' Log the memory usage to a local file
			m_MemoryUsageLogger.WriteMemoryUsageLogEntry()
		End If

		If Not m_BrokerDBLogger Is Nothing Then
			' Send the status info to the Broker DB
			' Note that m_BrokerDBLogger() only logs the status every x minutes (unless ForceLogToBrokerDB = True)

			LogStatusToBrokerDatabase(ForceLogToBrokerDB)
		End If
	End Sub

	Protected Function WriteStatusFileToDisk(ByRef strXMLText As String) As Boolean

		Const MIN_FILE_WRITE_INTERVAL_SECONDS As Integer = 2

		Static dtLastFileWriteTime As DateTime = DateTime.UtcNow

		Dim strTempStatusFilePath As String
		Dim blnSuccess As Boolean

		blnSuccess = True

		If DateTime.UtcNow.Subtract(dtLastFileWriteTime).TotalSeconds >= MIN_FILE_WRITE_INTERVAL_SECONDS Then
			' We will write out the Status XML to a temporary file, then rename the temp file to the primary file

			strTempStatusFilePath = Path.Combine(Path.GetDirectoryName(m_FileNamePath), Path.GetFileNameWithoutExtension(m_FileNamePath) & "_Temp.xml")

			dtLastFileWriteTime = DateTime.UtcNow

			blnSuccess = WriteStatusFileToDisk(strTempStatusFilePath, strXMLText)
			If blnSuccess Then
				Try
					File.Copy(strTempStatusFilePath, m_FileNamePath, True)
				Catch ex As Exception
					' Log a warning that the file copy failed
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Unable to copy temporary status file to the final status file (" &
					  Path.GetFileName(strTempStatusFilePath) & " to " & Path.GetFileName(m_FileNamePath) & "):" & ex.Message)
				End Try

				Try
					File.Delete(strTempStatusFilePath)
				Catch ex As Exception
					' Log a warning that the file delete failed
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Unable to delete temporary status file (" &
					  Path.GetFileName(strTempStatusFilePath) & "): " & ex.Message)
				End Try

			Else
				' Error writing to the temporary status file; try the primary file
				blnSuccess = WriteStatusFileToDisk(m_FileNamePath, strXMLText)
			End If
		End If

		Return blnSuccess

	End Function

	Protected Function WriteStatusFileToDisk(ByVal strFilePath As String, ByVal strXMLText As String) As Boolean
		Const WRITE_FAILURE_LOG_THRESHOLD As Integer = 5

		Static intWritingErrorCountSaved As Integer = 0

		Dim blnSuccess As Boolean

		Try
			' Write out the XML text to a file
			' If the file is in use by another process, then the writing will fail
			Using srOutFile = New StreamWriter(New FileStream(strFilePath, FileMode.Create, FileAccess.Write, FileShare.Read))
				srOutFile.WriteLine(strXMLText)
			End Using

			' Reset the error counter
			intWritingErrorCountSaved = 0

			blnSuccess = True

		Catch ex As Exception
			' Increment the error counter
			intWritingErrorCountSaved += 1

			If intWritingErrorCountSaved >= WRITE_FAILURE_LOG_THRESHOLD Then
				' 5 or more errors in a row have occurred
				' Post an entry to the log, only when intWritingErrorCountSaved is 5, 10, 20, 30, etc.
				If intWritingErrorCountSaved = WRITE_FAILURE_LOG_THRESHOLD OrElse intWritingErrorCountSaved Mod 10 = 0 Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error writing status file " & Path.GetFileName(strFilePath) & ": " & ex.Message)
				End If
			End If
			blnSuccess = False
		End Try

		Return blnSuccess

	End Function

	''' <summary>
	''' Updates status file
	''' </summary>
	''' <param name="JobInfo">Information on the job that started most recently</param>
	''' <param name="ForceLogToBrokerDB">If true, then will force m_BrokerDBLogger to report the manager status to the database</param>
	''' <remarks></remarks>
	Public Sub UpdateClose(
	  ByVal ManagerIdleMessage As String, _
	  ByRef RecentErrorMessages() As String, _
	  ByVal JobInfo As String, _
	  ByVal ForceLogToBrokerDB As Boolean) Implements IStatusFile.UpdateClose

		ClearCachedInfo()
		m_MgrStatus = IStatusFile.EnumMgrStatus.STOPPED
		m_TaskStatus = IStatusFile.EnumTaskStatus.NO_TASK
		m_TaskStatusDetail = IStatusFile.EnumTaskStatusDetail.NO_TASK
		m_MostRecentLogMessage = ManagerIdleMessage

		StoreRecentErrorMessages(RecentErrorMessages)
		StoreRecentJobInfo(JobInfo)

		Me.WriteStatusFile(ForceLogToBrokerDB)

	End Sub

	''' <summary>
	''' Updates status file
	''' </summary>
	''' <param name="PercentComplete">Job completion percentage (value between 0 and 100)</param>
	''' <remarks></remarks>
	Public Sub UpdateAndWrite(ByVal PercentComplete As Single) Implements IStatusFile.UpdateAndWrite

		m_Progress = PercentComplete
		Me.WriteStatusFile()

	End Sub

	''' <summary>
	''' Updates status file
	''' </summary>
	''' <param name="eMgrStatus">Job status enum</param>
	''' <param name="eTaskStatus">Task status enum</param>
	''' <param name="eTaskStatusDetail">Task status detail enum</param>
	''' <param name="PercentComplete">Job completion percentage (value between 0 and 100)</param>
	''' <remarks></remarks>
	Public Sub UpdateAndWrite(ByVal eMgrStatus As IStatusFile.EnumMgrStatus, ByVal eTaskStatus As IStatusFile.EnumTaskStatus, ByVal eTaskStatusDetail As IStatusFile.EnumTaskStatusDetail, ByVal PercentComplete As Single) Implements IStatusFile.UpdateAndWrite

		m_MgrStatus = eMgrStatus
		m_TaskStatus = eTaskStatus
		m_TaskStatusDetail = eTaskStatusDetail
		m_Progress = PercentComplete
		Me.WriteStatusFile()

	End Sub

	''' <summary>
	''' Updates status file
	''' </summary>
	''' <param name="Status">Job status enum</param>
	''' <param name="PercentComplete">Job completion percentage (value between 0 and 100)</param>
	''' <param name="SpectrumCountTotal">Number of DTA files (i.e., spectra files); relevant for Sequest, X!Tandem, and Inspect</param>
	''' <remarks></remarks>
	Public Sub UpdateAndWrite(ByVal Status As IStatusFile.EnumTaskStatus, ByVal PercentComplete As Single, ByVal SpectrumCountTotal As Integer) Implements IStatusFile.UpdateAndWrite

		m_TaskStatus = Status
		m_Progress = PercentComplete
		m_SpectrumCount = SpectrumCountTotal

		Me.WriteStatusFile()

	End Sub

	''' <summary>
	''' Updates status file
	''' </summary>
	''' <param name="eMgrStatus">Job status code</param>
	''' <param name="eTaskStatus">Task status code</param>
	''' <param name="eTaskStatusDetail">Detailed task status</param>
	''' <param name="PercentComplete">Job completion percentage (value between 0 and 100)</param>
	''' <param name="DTACount">Number of DTA files (i.e., spectra files); relevant for Sequest, X!Tandem, and Inspect</param>
	''' <param name="MostRecentLogMessage">Most recent message posted to the logger (leave blank if unknown)</param>
	''' <param name="MostRecentErrorMessage">Most recent error posted to the logger (leave blank if unknown)</param>
	''' <param name="RecentJobInfo">Information on the job that started most recently</param>
	''' <param name="ForceLogToBrokerDB">If true, then will force m_BrokerDBLogger to report the manager status to the database</param>
	''' <remarks></remarks>
	Public Sub UpdateAndWrite(
	  ByVal eMgrStatus As IStatusFile.EnumMgrStatus, _
	  ByVal eTaskStatus As IStatusFile.EnumTaskStatus, _
	  ByVal eTaskStatusDetail As IStatusFile.EnumTaskStatusDetail, _
	  ByVal PercentComplete As Single, _
	  ByVal DTACount As Integer, _
	  ByVal MostRecentLogMessage As String, _
	  ByVal MostRecentErrorMessage As String, _
	  ByVal RecentJobInfo As String, _
	  ByVal ForceLogToBrokerDB As Boolean) Implements IStatusFile.UpdateAndWrite

		m_MgrStatus = eMgrStatus
		m_TaskStatus = eTaskStatus
		m_TaskStatusDetail = eTaskStatusDetail
		m_Progress = PercentComplete
		m_SpectrumCount = DTACount

		m_MostRecentLogMessage = MostRecentLogMessage
		StoreNewErrorMessage(MostRecentErrorMessage, True)
		StoreRecentJobInfo(RecentJobInfo)

		Me.WriteStatusFile(ForceLogToBrokerDB)

	End Sub

	''' <summary>
	''' Sets status file to show mahager idle
	''' </summary>
	''' <remarks></remarks>
	Public Sub UpdateIdle() Implements IStatusFile.UpdateIdle
		UpdateIdle("Manager Idle", False)
	End Sub

	''' <summary>
	''' Logs to the status file that the manager is idle
	''' </summary>
	''' <param name="ManagerIdleMessage">Reason why the manager is idle (leave blank if unknown)</param>
	''' <param name="ForceLogToBrokerDB">If true, then will force m_BrokerDBLogger to report the manager status to the database</param>
	''' <remarks></remarks>
	Public Sub UpdateIdle(ByVal ManagerIdleMessage As String, ByVal ForceLogToBrokerDB As Boolean) Implements IStatusFile.UpdateIdle
		ClearCachedInfo()
		m_TaskStatus = IStatusFile.EnumTaskStatus.NO_TASK
		m_MostRecentLogMessage = ManagerIdleMessage

		Me.WriteStatusFile(ForceLogToBrokerDB)
	End Sub

	''' <summary>
	''' Logs to the status file that the manager is idle
	''' </summary>
	''' <param name="ManagerIdleMessage">Reason why the manager is idle (leave blank if unknown)</param>
	''' <param name="IdleErrorMessage">Error message explaining why the manager is idle</param>
	''' <param name="RecentJobInfo">Information on the job that started most recently</param>
	''' <param name="ForceLogToBrokerDB">If true, then will force m_BrokerDBLogger to report the manager status to the database</param>
	''' <remarks></remarks>
	Public Sub UpdateIdle(
	  ByVal ManagerIdleMessage As String, _
	  ByVal IdleErrorMessage As String, _
	  ByVal RecentJobInfo As String, _
	  ByVal ForceLogToBrokerDB As Boolean) Implements IStatusFile.UpdateIdle
		ClearCachedInfo()
		m_TaskStatus = IStatusFile.EnumTaskStatus.NO_TASK
		m_MostRecentLogMessage = ManagerIdleMessage

		StoreNewErrorMessage(IdleErrorMessage, True)
		StoreRecentJobInfo(RecentJobInfo)

		Me.WriteStatusFile(ForceLogToBrokerDB)

	End Sub

	''' <summary>
	''' Logs to the status file that the manager is idle
	''' </summary>
	''' <param name="ManagerIdleMessage">Reason why the manager is idle (leave blank if unknown)</param>
	''' <param name="RecentErrorMessages">Recent error messages written to the log file (leave blank if unknown)</param>
	''' <param name="RecentJobInfo">Information on the job that started most recently</param>
	''' <param name="ForceLogToBrokerDB">If true, then will force m_BrokerDBLogger to report the manager status to the database</param>
	''' <remarks></remarks>
	Public Sub UpdateIdle(
	  ByVal ManagerIdleMessage As String, _
	  ByRef RecentErrorMessages() As String, _
	  ByVal RecentJobInfo As String, _
	  ByVal ForceLogToBrokerDB As Boolean) Implements IStatusFile.UpdateIdle

		ClearCachedInfo()
		m_MgrStatus = IStatusFile.EnumMgrStatus.RUNNING
		m_TaskStatus = IStatusFile.EnumTaskStatus.NO_TASK
		m_TaskStatusDetail = IStatusFile.EnumTaskStatusDetail.NO_TASK
		m_MostRecentLogMessage = ManagerIdleMessage

		StoreRecentErrorMessages(RecentErrorMessages)
		StoreRecentJobInfo(RecentJobInfo)

		Me.WriteStatusFile(ForceLogToBrokerDB)
	End Sub

	''' <summary>
	''' Updates status file to show manager disabled
	''' </summary>
	''' <remarks></remarks>
	Public Sub UpdateDisabled(ByVal ManagerStatus As IStatusFile.EnumMgrStatus) Implements IStatusFile.UpdateDisabled
		UpdateDisabled(ManagerStatus, "Manager Disabled")
	End Sub

	''' <summary>
	''' Logs to the status file that the manager is disabled (either in the manager control DB or via the local AnalysisManagerProg.exe.config file)
	''' </summary>
	''' <param name="ManagerDisableMessage">Description of why the manager is disabled (leave blank if unknown)</param>
	''' <remarks></remarks>
	Public Sub UpdateDisabled(ByVal ManagerStatus As IStatusFile.EnumMgrStatus, ByVal ManagerDisableMessage As String) Implements IStatusFile.UpdateDisabled
		Dim strRecentErrorMessages() As String
		ReDim strRecentErrorMessages(-1)

		UpdateDisabled(ManagerStatus, ManagerDisableMessage, strRecentErrorMessages, m_MostRecentJobInfo)
	End Sub

	''' <summary>
	''' Logs to the status file that the manager is disabled (either in the manager control DB or via the local AnalysisManagerProg.exe.config file)
	''' </summary>
	''' <param name="ManagerDisableMessage">Description of why the manager is disabled (leave blank if unknown)</param>
	''' <param name="RecentErrorMessages">Recent error messages written to the log file (leave blank if unknown)</param>
	''' <param name="RecentJobInfo">Information on the job that started most recently</param>
	''' <remarks></remarks>
	Public Sub UpdateDisabled(ByVal ManagerStatus As IStatusFile.EnumMgrStatus, ByVal ManagerDisableMessage As String, ByRef RecentErrorMessages() As String, ByVal RecentJobInfo As String) Implements IStatusFile.UpdateDisabled
		ClearCachedInfo()

		If Not (ManagerStatus = IStatusFile.EnumMgrStatus.DISABLED_LOCAL OrElse ManagerStatus = IStatusFile.EnumMgrStatus.DISABLED_MC) Then
			ManagerStatus = IStatusFile.EnumMgrStatus.DISABLED_LOCAL
		End If
		m_MgrStatus = ManagerStatus
		m_TaskStatus = IStatusFile.EnumTaskStatus.NO_TASK
		m_TaskStatusDetail = IStatusFile.EnumTaskStatusDetail.NO_TASK
		m_MostRecentLogMessage = ManagerDisableMessage

		StoreRecentJobInfo(RecentJobInfo)
		StoreRecentErrorMessages(RecentErrorMessages)

		Me.WriteStatusFile(True)
	End Sub

	''' <summary>
	''' Updates status file to show manager stopped due to a flag file
	''' </summary>
	''' <remarks></remarks>
	Public Sub UpdateFlagFileExists() Implements IStatusFile.UpdateFlagFileExists
		Dim strRecentErrorMessages() As String
		ReDim strRecentErrorMessages(-1)

		UpdateFlagFileExists(strRecentErrorMessages, m_MostRecentJobInfo)
	End Sub

	''' <summary>
	''' Logs to the status file that a flag file exists, indicating that the manager did not exit cleanly on a previous run
	''' </summary>
	''' <param name="RecentErrorMessages">Recent error messages written to the log file (leave blank if unknown)</param>
	''' <param name="RecentJobInfo">Information on the job that started most recently</param>
	''' <remarks></remarks>
	Public Sub UpdateFlagFileExists(ByRef RecentErrorMessages() As String, ByVal RecentJobInfo As String) Implements IStatusFile.UpdateFlagFileExists
		ClearCachedInfo()

		m_MgrStatus = IStatusFile.EnumMgrStatus.STOPPED_ERROR
		m_MostRecentLogMessage = "Flag file"
		StoreRecentErrorMessages(RecentErrorMessages)
		StoreRecentJobInfo(RecentJobInfo)

		Me.WriteStatusFile(True)
	End Sub

	''' <summary>
	''' Total time the job has been running
	''' </summary>
	''' <returns>Number of hours manager has been processing job</returns>
	''' <remarks></remarks>
	Private Function GetRunTime() As Single

		Return CSng(DateTime.UtcNow.Subtract(m_TaskStartTime).TotalHours)

	End Function

	Public Sub DisposeMessageQueue()
		If Not m_MessageSender Is Nothing Then
			m_QueueLogger.Dispose()
			m_MessageSender.Dispose()
		End If

	End Sub

#End Region

End Class


