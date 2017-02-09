'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
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

    ''' <summary>
    ''' System-wide free memory
    ''' </summary>
    ''' <remarks></remarks>
    Private m_FreeMemoryMB As Single = 0

    ' Flag to indicate that the ABORT_PROCESSING_NOW_FILENAME file was detected
    Private m_AbortProcessingNow As Boolean = False

    Private m_MostRecentLogMessage As String

    Const MAX_ERROR_MESSAGE_COUNT_TO_CACHE As Integer = 10
    Private m_RecentErrorMessageCount As Integer

    ' ReSharper disable once FieldCanBeMadeReadOnly.Local
    Private m_RecentErrorMessages(MAX_ERROR_MESSAGE_COUNT_TO_CACHE - 1) As String

    Private m_ProgRunnerCoreUsageHistory As Queue(Of KeyValuePair(Of DateTime, Single))

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
    Public Property FileNamePath As String Implements IStatusFile.FileNamePath

    Public Property MgrName As String Implements IStatusFile.MgrName

    Public Property MgrStatus As IStatusFile.EnumMgrStatus Implements IStatusFile.MgrStatus

    ''' <summary>
    ''' Overall CPU utilization of all threads
    ''' </summary>
    ''' <remarks></remarks>
    Public Property CpuUtilization As Integer Implements IStatusFile.CpuUtilization

    ''' <summary>
    ''' System-wide free memory
    ''' </summary>
    ''' <remarks></remarks>
    Public ReadOnly Property FreeMemoryMB() As Single
        Get
            Return m_FreeMemoryMB
        End Get
    End Property

    Public Property Tool As String Implements IStatusFile.Tool

    Public Property TaskStatus As IStatusFile.EnumTaskStatus Implements IStatusFile.TaskStatus

    Public Property TaskStartTime As DateTime

    ''' <summary>
    ''' Progress (value between 0 and 100)
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Property Progress As Single Implements IStatusFile.Progress

    ''' <summary>
    ''' ProcessID of an externally spawned process
    ''' </summary>
    ''' <remarks>0 if no external process running</remarks>
    Public Property ProgRunnerProcessID As Integer Implements IStatusFile.ProgRunnerProcessID

    ''' <summary>
    ''' Number of cores in use by an externally spawned process
    ''' </summary>
    ''' <remarks></remarks>
    Public Property ProgRunnerCoreUsage As Single Implements IStatusFile.ProgRunnerCoreUsage

    Public Property CurrentOperation As String Implements IStatusFile.CurrentOperation

    Public Property TaskStatusDetail As IStatusFile.EnumTaskStatusDetail Implements IStatusFile.TaskStatusDetail

    Public Property JobNumber As Integer Implements IStatusFile.JobNumber

    Public Property JobStep As Integer Implements IStatusFile.JobStep

    ''' <summary>
    ''' Dataset name
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Property Dataset As String Implements IStatusFile.Dataset

    ''' <summary>
    ''' Most recent job info
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Property MostRecentJobInfo As String Implements IStatusFile.MostRecentJobInfo

    ''' <summary>
    ''' Number of spectrum files created
    ''' </summary>
    ''' <value></value>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Property SpectrumCount As Integer Implements IStatusFile.SpectrumCount

    Public Property MessageQueueURI As String Implements IStatusFile.MessageQueueURI

    Public Property MessageQueueTopic As String Implements IStatusFile.MessageQueueTopic

    Public Property LogToMsgQueue As Boolean Implements IStatusFile.LogToMsgQueue

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
    Public Sub New(FileLocation As String, debugLevel As Integer)
        FileNamePath = FileLocation
        MgrName = String.Empty
        MgrStatus = IStatusFile.EnumMgrStatus.STOPPED

        TaskStatus = IStatusFile.EnumTaskStatus.NO_TASK
        TaskStatusDetail = IStatusFile.EnumTaskStatusDetail.NO_TASK
        TaskStartTime = Date.UtcNow

        Dataset = String.Empty
        CurrentOperation = String.Empty
        MostRecentJobInfo = String.Empty

        m_DebugLevel = debugLevel

        ClearCachedInfo()

        InitializePerformanceCounters()
    End Sub

    ''' <summary>
    ''' Configure the memory logging settings
    ''' </summary>
    ''' <param name="logMemoryUsage"></param>
    ''' <param name="minimumMemoryUsageLogIntervalMinutes"></param>
    ''' <param name="memoryUsageLogFolderPath"></param>
    ''' <remarks></remarks>
    Public Sub ConfigureMemoryLogging(logMemoryUsage As Boolean, minimumMemoryUsageLogIntervalMinutes As Single, memoryUsageLogFolderPath As String)
        If logMemoryUsage Then
            If m_MemoryUsageLogger Is Nothing Then
                m_MemoryUsageLogger = New clsMemoryUsageLogger(memoryUsageLogFolderPath, minimumMemoryUsageLogIntervalMinutes)
            Else
                m_MemoryUsageLogger.MinimumLogIntervalMinutes = minimumMemoryUsageLogIntervalMinutes
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
    Public Sub ConfigureBrokerDBLogging(LogStatusToBrokerDB As Boolean, BrokerDBConnectionString As String, BrokerDBStatusUpdateIntervalMinutes As Single)
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
    Public Sub ConfigureMessageQueueLogging(
      LogStatusToMessageQueue As Boolean,
      MsgQueueURI As String,
      MessageQueueTopicMgrStatus As String,
      ClientName As String)

        LogToMsgQueue = LogStatusToMessageQueue
        MessageQueueURI = MsgQueueURI
        MessageQueueTopic = MessageQueueTopicMgrStatus

        If Not LogToMsgQueue And Not m_MessageQueueLogger Is Nothing Then
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
            strPathToCheck = Path.GetDirectoryName(FileNamePath)
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
        Progress = 0
        SpectrumCount = 0
        Dataset = ""
        JobNumber = 0
        JobStep = 0
        Tool = ""

        ProgRunnerProcessID = 0
        ProgRunnerCoreUsage = 0

        ' Only clear the recent job info if the variable is Nothing
        If MostRecentJobInfo Is Nothing Then
            MostRecentJobInfo = String.Empty
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
    Private Function ConvertTaskStatusToString(StatusEnum As IStatusFile.EnumTaskStatus) As String

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
    Private Function ConvertMgrStatusToString(StatusEnum As IStatusFile.EnumMgrStatus) As String

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
    Private Function ConvertTaskStatusDetailToString(StatusEnum As IStatusFile.EnumTaskStatusDetail) As String

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
            If Not blnVirtualMachineOnPIC AndAlso Date.Now().Hour = 0 AndAlso Date.Now().Minute <= 30 Then
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
            If Not blnVirtualMachineOnPIC AndAlso Date.Now().Hour = 0 AndAlso Date.Now().Minute <= 30 Then
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error instantiating the Memory.[Available MBytes] performance counter (this message is only logged between 12 am and 12:30 am): " & ex.Message)
            End If

        End Try
    End Sub

    ''' <summary>
    ''' Returns the CPU usage
    ''' </summary>
    ''' <returns>Value between 0 and 100</returns>
    ''' <remarks>
    ''' This is CPU usage for all running applications, not just this application
    ''' For CPU usage of a single application use PRISM.Processes.clsProgRunner.GetCoreUsageByProcessID()
    ''' </remarks>
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
    ''' Returns the number of cores
    ''' </summary>
    ''' <returns>The number of cores on this computer</returns>
    ''' <remarks>Should not be affected by hyperthreading, so a computer with two 4-core chips will report 8 cores</remarks>
    Public Function GetCoreCount() As Integer Implements IStatusFile.GetCoreCount

        Return PRISM.Processes.clsProgRunner.GetCoreCount()

    End Function

    ''' <summary>
    ''' Returns the amount of free memory
    ''' </summary>
    ''' <returns>Amount of free memory, in MB</returns>
    Public Function GetFreeMemoryMB() As Single Implements IStatusFile.GetFreeMemoryMB
        Dim sngFreeMemory As Single = 0

        Try
            If Not mFreeMemoryPerformanceCounter Is Nothing Then
                sngFreeMemory = mFreeMemoryPerformanceCounter.NextValue()
            End If

            If sngFreeMemory < Single.Epsilon Then
                sngFreeMemory = CSng(clsGlobal.BytesToMB(CLng(New Devices.ComputerInfo().AvailablePhysicalMemory)))
            End If

        Catch ex As Exception
            ' Ignore errors here
        End Try

        Return sngFreeMemory

    End Function

    ''' <summary>
    ''' Return the ProcessID of the Analysis manager
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Public Function GetProcessID() As Integer Implements IStatusFile.GetProcessID
        Dim processID = Process.GetCurrentProcess().Id
        Return processID
    End Function

    Protected Sub LogStatusToMessageQueue(strStatusXML As String)

        Const MINIMUM_LOG_FAILURE_INTERVAL_MINUTES As Single = 10
        Static dtLastFailureTime As DateTime = Date.UtcNow.Subtract(New TimeSpan(1, 0, 0))

        Try
            If m_MessageSender Is Nothing Then

                If m_DebugLevel >= 5 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Initializing message queue with URI '" & MessageQueueURI & "' and Topic '" & MessageQueueTopic & "'")
                End If

                m_MessageSender = New clsMessageSender(MessageQueueURI, MessageQueueTopic, MgrName)

                ' message queue logger sets up local message buffering (so calls to log don't block)
                ' and uses message sender (as a delegate) to actually send off the messages
                m_QueueLogger = New clsMessageQueueLogger()
                AddHandler m_QueueLogger.Sender, New MessageSenderDelegate(AddressOf m_MessageSender.SendMessage)

                If m_DebugLevel >= 3 Then
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Message queue initialized with URI '" & MessageQueueURI & "'; posting to Topic '" & MessageQueueTopic & "'")
                End If

            End If

            If Not m_QueueLogger Is Nothing Then
                m_QueueLogger.LogStatusMessage(strStatusXML)
            End If

        Catch ex As Exception
            If Date.UtcNow.Subtract(dtLastFailureTime).TotalMinutes >= MINIMUM_LOG_FAILURE_INTERVAL_MINUTES Then
                dtLastFailureTime = Date.UtcNow
                Dim msg = "Error in clsStatusFile.LogStatusToMessageQueue (B): " & ex.Message
                Console.WriteLine(msg)
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg)
            End If

        End Try


    End Sub

    ''' <summary>
    ''' Send status information to the database
    ''' </summary>
    ''' <remarks>This function is valid, but the primary way that we track status is when WriteStatusFile calls LogStatusToMessageQueue</remarks>
    Protected Sub LogStatusToBrokerDatabase(ForceLogToBrokerDB As Boolean)

        Dim intIndex As Integer

        Dim udtStatusInfo = New clsDBStatusLogger.udtStatusInfoType() With {
            .MgrName = MgrName,
            .MgrStatus = MgrStatus,
            .LastUpdate = Date.UtcNow(),
            .LastStartTime = TaskStartTime,
            .CPUUtilization = CpuUtilization,
            .FreeMemoryMB = m_FreeMemoryMB,
            .ProcessID = GetProcessID(),
            .ProgRunnerProcessID = ProgRunnerProcessID,
            .ProgRunnerCoreUsage = ProgRunnerCoreUsage
        }

        If m_RecentErrorMessageCount = 0 Then
            udtStatusInfo.MostRecentErrorMessage = String.Empty
        Else
            udtStatusInfo.MostRecentErrorMessage = m_RecentErrorMessages(0)
            If m_RecentErrorMessageCount > 1 Then
                ' Append the next two error messages
                For intIndex = 1 To m_RecentErrorMessageCount - 1
                    udtStatusInfo.MostRecentErrorMessage &= Environment.NewLine & m_RecentErrorMessages(intIndex)
                    If intIndex >= 2 Then Exit For
                Next
            End If
        End If

        Dim udtTask = New clsDBStatusLogger.udtTaskInfoType() With {
            .Tool = Tool,
            .Status = TaskStatus,
            .DurationHours = GetRunTime(),
            .Progress = Progress,
            .CurrentOperation = CurrentOperation
        }

        Dim udtTaskDetails = New clsDBStatusLogger.udtTaskDetailsType() With {
            .Status = TaskStatusDetail,
            .Job = JobNumber,
            .JobStep = JobStep,
            .Dataset = Dataset,
            .MostRecentLogMessage = m_MostRecentLogMessage,
            .MostRecentJobInfo = MostRecentJobInfo,
            .SpectrumCount = SpectrumCount
        }

        udtTask.TaskDetails = udtTaskDetails
        udtStatusInfo.Task = udtTask


        m_BrokerDBLogger.LogStatus(udtStatusInfo, ForceLogToBrokerDB)
    End Sub

    Public Sub StoreCoreUsageHistory(coreUsageHistory As Queue(Of KeyValuePair(Of DateTime, Single))) Implements IStatusFile.StoreCoreUsageHistory
        m_ProgRunnerCoreUsageHistory = coreUsageHistory
    End Sub

    Protected Sub StoreRecentJobInfo(JobInfo As String)
        If Not JobInfo Is Nothing AndAlso JobInfo.Length > 0 Then
            MostRecentJobInfo = JobInfo
        End If
    End Sub

    Protected Sub StoreNewErrorMessage(strErrorMessage As String, blnClearExistingMessages As Boolean)
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
    Public Sub WriteStatusFile(ForceLogToBrokerDB As Boolean) Implements IStatusFile.WriteStatusFile

        'Writes a status file for external monitor to read

        Dim strXMLText As String = String.Empty

        Dim dtLastUpdate As DateTime
        Dim sngRunTimeHours As Single

        Try
            dtLastUpdate = Date.UtcNow()
            sngRunTimeHours = GetRunTime()

            CpuUtilization = CInt(GetCPUUtilization())
            m_FreeMemoryMB = GetFreeMemoryMB()
        Catch ex As Exception
            ' Ignore errors here
        End Try

        ' Set up the XML writer
        Try
            ' Create a new memory stream in which to write the XML
            Dim objMemoryStream = New MemoryStream
            Using xWriter = New XmlTextWriter(objMemoryStream, Text.Encoding.UTF8)

                xWriter.Formatting = Formatting.Indented
                xWriter.Indentation = 2

                'Create the XML document in memory
                xWriter.WriteStartDocument(True)
                xWriter.WriteComment("Analysis manager job status")

                'General job information
                'Root level element
                xWriter.WriteStartElement("Root")    ' Root
                xWriter.WriteStartElement("Manager")  ' Manager
                xWriter.WriteElementString("MgrName", MgrName)
                xWriter.WriteElementString("MgrStatus", ConvertMgrStatusToString(MgrStatus))
                xWriter.WriteElementString("LastUpdate", dtLastUpdate.ToLocalTime().ToString())
                xWriter.WriteElementString("LastStartTime", TaskStartTime.ToLocalTime().ToString())
                xWriter.WriteElementString("CPUUtilization", CpuUtilization.ToString("##0.0"))
                xWriter.WriteElementString("FreeMemoryMB", m_FreeMemoryMB.ToString("##0.0"))
                xWriter.WriteElementString("ProcessID", GetProcessID().ToString())
                xWriter.WriteElementString("ProgRunnerProcessID", ProgRunnerProcessID.ToString())
                xWriter.WriteElementString("ProgRunnerCoreUsage", ProgRunnerCoreUsage.ToString("0.00"))
                xWriter.WriteStartElement("RecentErrorMessages")
                If m_RecentErrorMessageCount = 0 Then
                    xWriter.WriteElementString("ErrMsg", String.Empty)
                Else
                    For intErrorMsgIndex As Integer = 0 To m_RecentErrorMessageCount - 1
                        xWriter.WriteElementString("ErrMsg", m_RecentErrorMessages(intErrorMsgIndex))
                    Next
                End If
                xWriter.WriteEndElement()               ' RecentErrorMessages
                xWriter.WriteEndElement()               ' Manager

                xWriter.WriteStartElement("Task")       ' Task
                xWriter.WriteElementString("Tool", Tool)
                xWriter.WriteElementString("Status", ConvertTaskStatusToString(TaskStatus))
                xWriter.WriteElementString("Duration", sngRunTimeHours.ToString("0.00"))
                xWriter.WriteElementString("DurationMinutes", (sngRunTimeHours * 60).ToString("0.0"))
                xWriter.WriteElementString("Progress", Progress.ToString("##0.00"))
                xWriter.WriteElementString("CurrentOperation", CurrentOperation)

                xWriter.WriteStartElement("TaskDetails") 'TaskDetails
                xWriter.WriteElementString("Status", ConvertTaskStatusDetailToString(TaskStatusDetail))
                xWriter.WriteElementString("Job", CStr(JobNumber))
                xWriter.WriteElementString("Step", CStr(JobStep))
                xWriter.WriteElementString("Dataset", Dataset)
                xWriter.WriteElementString("MostRecentLogMessage", m_MostRecentLogMessage)
                xWriter.WriteElementString("MostRecentJobInfo", MostRecentJobInfo)
                xWriter.WriteElementString("SpectrumCount", SpectrumCount.ToString)
                xWriter.WriteEndElement()               ' TaskDetails
                xWriter.WriteEndElement()               ' Task

                If ProgRunnerProcessID <> 0 AndAlso Not m_ProgRunnerCoreUsageHistory Is Nothing Then
                    xWriter.WriteStartElement("ProgRunnerCoreUsage")
                    xWriter.WriteAttributeString("Count", m_ProgRunnerCoreUsageHistory.Count.ToString())
                    For Each coreUsageSample In m_ProgRunnerCoreUsageHistory
                        xWriter.WriteStartElement("CoreUsageSample")
                        xWriter.WriteAttributeString("Date", coreUsageSample.Key.ToString("yyyy-MM-dd hh:mm:ss tt"))
                        xWriter.WriteValue(coreUsageSample.Value.ToString("0.0"))
                        xWriter.WriteEndElement()       ' CoreUsageSample
                    Next
                    xWriter.WriteEndElement()           ' ProgRunnerCoreUsage
                End If

                xWriter.WriteEndElement()               ' Root

                ' Close out the XML document (but do not close XWriter yet)
                xWriter.WriteEndDocument()
                xWriter.Flush()

                ' Now use a StreamReader to copy the XML text to a string variable
                objMemoryStream.Seek(0, SeekOrigin.Begin)
                Dim srMemoryStreamReader = New StreamReader(objMemoryStream)
                strXMLText = srMemoryStreamReader.ReadToEnd

                srMemoryStreamReader.Close()
                objMemoryStream.Close()

                ' Since strXMLText now contains the XML, we can now safely close XWriter
            End Using

            WriteStatusFileToDisk(strXMLText)

        Catch ex As Exception
            Dim msg = "Error generating status info: " & ex.Message
            Console.WriteLine(msg)
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg)
        End Try

        CheckForAbortProcessingFile()

        If LogToMsgQueue Then
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

        Const MIN_FILE_WRITE_INTERVAL_SECONDS = 2

        Static dtLastFileWriteTime As DateTime = Date.UtcNow

        Dim strTempStatusFilePath As String
        Dim blnSuccess As Boolean

        blnSuccess = True

        If Date.UtcNow.Subtract(dtLastFileWriteTime).TotalSeconds >= MIN_FILE_WRITE_INTERVAL_SECONDS Then
            ' We will write out the Status XML to a temporary file, then rename the temp file to the primary file

            strTempStatusFilePath = Path.Combine(Path.GetDirectoryName(FileNamePath), Path.GetFileNameWithoutExtension(FileNamePath) & "_Temp.xml")

            dtLastFileWriteTime = Date.UtcNow

            Dim logWarning = True
            If Tool.ToLower().Contains("glyq") OrElse Tool.ToLower().Contains("modplus") Then
                If m_DebugLevel < 3 Then logWarning = False
            End If

            blnSuccess = WriteStatusFileToDisk(strTempStatusFilePath, strXMLText, logWarning)
            If blnSuccess Then
                Try
                    File.Copy(strTempStatusFilePath, FileNamePath, True)
                Catch ex As Exception
                    ' Copy failed; this is normal when running GlyQ-IQ or MODPlus because they have multiple threads running                  
                    If logWarning Then
                        ' Log a warning that the file copy failed
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                                             "Unable to copy temporary status file to the final status file (" &
                                             Path.GetFileName(strTempStatusFilePath) & " to " &
                                             Path.GetFileName(FileNamePath) & "):" & ex.Message)
                    End If

                End Try

                Try
                    File.Delete(strTempStatusFilePath)
                Catch ex As Exception
                    ' Delete failed; this is normal when running GlyQ-IQ or MODPlus because they have multiple threads running                  
                    If logWarning Then
                        ' Log a warning that the file delete failed
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Unable to delete temporary status file (" &
                          Path.GetFileName(strTempStatusFilePath) & "): " & ex.Message)
                    End If
                End Try

            Else
                ' Error writing to the temporary status file; try the primary file
                blnSuccess = WriteStatusFileToDisk(FileNamePath, strXMLText, logWarning)
            End If
        End If

        Return blnSuccess

    End Function

    Protected Function WriteStatusFileToDisk(strFilePath As String, strXMLText As String, logWarning As Boolean) As Boolean
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

            If intWritingErrorCountSaved >= WRITE_FAILURE_LOG_THRESHOLD AndAlso logWarning Then
                ' 5 or more errors in a row have occurred
                ' Post an entry to the log, only when intWritingErrorCountSaved is 5, 10, 20, 30, etc.
                If intWritingErrorCountSaved = WRITE_FAILURE_LOG_THRESHOLD OrElse intWritingErrorCountSaved Mod 10 = 0 Then
                    Dim msg = "Error writing status file " & Path.GetFileName(strFilePath) & ": " & ex.Message
                    Console.WriteLine(msg)
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg)
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
      ManagerIdleMessage As String,
      ByRef RecentErrorMessages() As String,
      JobInfo As String,
      ForceLogToBrokerDB As Boolean) Implements IStatusFile.UpdateClose

        ClearCachedInfo()

        MgrStatus = IStatusFile.EnumMgrStatus.STOPPED
        TaskStatus = IStatusFile.EnumTaskStatus.NO_TASK
        TaskStatusDetail = IStatusFile.EnumTaskStatusDetail.NO_TASK
        m_MostRecentLogMessage = ManagerIdleMessage

        StoreRecentErrorMessages(RecentErrorMessages)
        StoreRecentJobInfo(JobInfo)

        WriteStatusFile(ForceLogToBrokerDB)

    End Sub

    ''' <summary>
    ''' Updates status file
    ''' </summary>
    ''' <param name="PercentComplete">Job completion percentage (value between 0 and 100)</param>
    ''' <remarks></remarks>
    Public Sub UpdateAndWrite(PercentComplete As Single) Implements IStatusFile.UpdateAndWrite

        Progress = PercentComplete
        WriteStatusFile()

    End Sub

    ''' <summary>
    ''' Updates status file
    ''' </summary>
    ''' <param name="eMgrStatus">Job status enum</param>
    ''' <param name="eTaskStatus">Task status enum</param>
    ''' <param name="eTaskStatusDetail">Task status detail enum</param>
    ''' <param name="PercentComplete">Job completion percentage (value between 0 and 100)</param>
    ''' <remarks></remarks>
    Public Sub UpdateAndWrite(
      eMgrStatus As IStatusFile.EnumMgrStatus,
      eTaskStatus As IStatusFile.EnumTaskStatus,
      eTaskStatusDetail As IStatusFile.EnumTaskStatusDetail,
      PercentComplete As Single) Implements IStatusFile.UpdateAndWrite

        MgrStatus = eMgrStatus
        TaskStatus = eTaskStatus
        TaskStatusDetail = eTaskStatusDetail
        Progress = PercentComplete
        WriteStatusFile()

    End Sub

    ''' <summary>
    ''' Updates status file
    ''' </summary>
    ''' <param name="Status">Job status enum</param>
    ''' <param name="PercentComplete">Job completion percentage (value between 0 and 100)</param>
    ''' <param name="SpectrumCountTotal">Number of DTA files (i.e., spectra files); relevant for Sequest, X!Tandem, and Inspect</param>
    ''' <remarks></remarks>
    Public Sub UpdateAndWrite(
      Status As IStatusFile.EnumTaskStatus,
      PercentComplete As Single,
      SpectrumCountTotal As Integer) Implements IStatusFile.UpdateAndWrite

        TaskStatus = Status
        Progress = PercentComplete
        SpectrumCount = SpectrumCountTotal

        WriteStatusFile()

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
      eMgrStatus As IStatusFile.EnumMgrStatus,
      eTaskStatus As IStatusFile.EnumTaskStatus,
      eTaskStatusDetail As IStatusFile.EnumTaskStatusDetail,
      PercentComplete As Single,
      DTACount As Integer,
      MostRecentLogMessage As String,
      MostRecentErrorMessage As String,
      RecentJobInfo As String,
      ForceLogToBrokerDB As Boolean) Implements IStatusFile.UpdateAndWrite

        MgrStatus = eMgrStatus
        TaskStatus = eTaskStatus
        TaskStatusDetail = eTaskStatusDetail
        Progress = PercentComplete
        SpectrumCount = DTACount

        m_MostRecentLogMessage = MostRecentLogMessage
        StoreNewErrorMessage(MostRecentErrorMessage, True)
        StoreRecentJobInfo(RecentJobInfo)

        WriteStatusFile(ForceLogToBrokerDB)

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
    Public Sub UpdateIdle(ManagerIdleMessage As String, ForceLogToBrokerDB As Boolean) Implements IStatusFile.UpdateIdle
        ClearCachedInfo()
        TaskStatus = IStatusFile.EnumTaskStatus.NO_TASK
        m_MostRecentLogMessage = ManagerIdleMessage

        WriteStatusFile(ForceLogToBrokerDB)
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
      ManagerIdleMessage As String,
      IdleErrorMessage As String,
      RecentJobInfo As String,
      ForceLogToBrokerDB As Boolean) Implements IStatusFile.UpdateIdle
        ClearCachedInfo()
        TaskStatus = IStatusFile.EnumTaskStatus.NO_TASK
        m_MostRecentLogMessage = ManagerIdleMessage

        StoreNewErrorMessage(IdleErrorMessage, True)
        StoreRecentJobInfo(RecentJobInfo)

        WriteStatusFile(ForceLogToBrokerDB)

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
      ManagerIdleMessage As String,
      ByRef RecentErrorMessages() As String,
      RecentJobInfo As String,
      ForceLogToBrokerDB As Boolean) Implements IStatusFile.UpdateIdle

        ClearCachedInfo()
        MgrStatus = IStatusFile.EnumMgrStatus.RUNNING
        TaskStatus = IStatusFile.EnumTaskStatus.NO_TASK
        TaskStatusDetail = IStatusFile.EnumTaskStatusDetail.NO_TASK
        m_MostRecentLogMessage = ManagerIdleMessage

        StoreRecentErrorMessages(RecentErrorMessages)
        StoreRecentJobInfo(RecentJobInfo)

        WriteStatusFile(ForceLogToBrokerDB)
    End Sub

    ''' <summary>
    ''' Updates status file to show manager disabled
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub UpdateDisabled(ManagerStatus As IStatusFile.EnumMgrStatus) Implements IStatusFile.UpdateDisabled
        UpdateDisabled(ManagerStatus, "Manager Disabled")
    End Sub

    ''' <summary>
    ''' Logs to the status file that the manager is disabled (either in the manager control DB or via the local AnalysisManagerProg.exe.config file)
    ''' </summary>
    ''' <param name="ManagerDisableMessage">Description of why the manager is disabled (leave blank if unknown)</param>
    ''' <remarks></remarks>
    Public Sub UpdateDisabled(ManagerStatus As IStatusFile.EnumMgrStatus, ManagerDisableMessage As String) Implements IStatusFile.UpdateDisabled
        Dim strRecentErrorMessages() As String
        ReDim strRecentErrorMessages(-1)

        UpdateDisabled(ManagerStatus, ManagerDisableMessage, strRecentErrorMessages, MostRecentJobInfo)
    End Sub

    ''' <summary>
    ''' Logs to the status file that the manager is disabled (either in the manager control DB or via the local AnalysisManagerProg.exe.config file)
    ''' </summary>
    ''' <param name="ManagerDisableMessage">Description of why the manager is disabled (leave blank if unknown)</param>
    ''' <param name="RecentErrorMessages">Recent error messages written to the log file (leave blank if unknown)</param>
    ''' <param name="RecentJobInfo">Information on the job that started most recently</param>
    ''' <remarks></remarks>
    Public Sub UpdateDisabled(ManagerStatus As IStatusFile.EnumMgrStatus, ManagerDisableMessage As String, ByRef RecentErrorMessages() As String, RecentJobInfo As String) Implements IStatusFile.UpdateDisabled
        ClearCachedInfo()

        If Not (ManagerStatus = IStatusFile.EnumMgrStatus.DISABLED_LOCAL OrElse ManagerStatus = IStatusFile.EnumMgrStatus.DISABLED_MC) Then
            ManagerStatus = IStatusFile.EnumMgrStatus.DISABLED_LOCAL
        End If
        MgrStatus = ManagerStatus
        TaskStatus = IStatusFile.EnumTaskStatus.NO_TASK
        TaskStatusDetail = IStatusFile.EnumTaskStatusDetail.NO_TASK
        m_MostRecentLogMessage = ManagerDisableMessage

        StoreRecentJobInfo(RecentJobInfo)
        StoreRecentErrorMessages(RecentErrorMessages)

        WriteStatusFile(True)
    End Sub

    ''' <summary>
    ''' Updates status file to show manager stopped due to a flag file
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub UpdateFlagFileExists() Implements IStatusFile.UpdateFlagFileExists
        Dim strRecentErrorMessages() As String
        ReDim strRecentErrorMessages(-1)

        UpdateFlagFileExists(strRecentErrorMessages, MostRecentJobInfo)
    End Sub

    ''' <summary>
    ''' Logs to the status file that a flag file exists, indicating that the manager did not exit cleanly on a previous run
    ''' </summary>
    ''' <param name="RecentErrorMessages">Recent error messages written to the log file (leave blank if unknown)</param>
    ''' <param name="RecentJobInfo">Information on the job that started most recently</param>
    ''' <remarks></remarks>
    Public Sub UpdateFlagFileExists(ByRef RecentErrorMessages() As String, RecentJobInfo As String) Implements IStatusFile.UpdateFlagFileExists
        ClearCachedInfo()

        MgrStatus = IStatusFile.EnumMgrStatus.STOPPED_ERROR
        m_MostRecentLogMessage = "Flag file"
        StoreRecentErrorMessages(RecentErrorMessages)
        StoreRecentJobInfo(RecentJobInfo)

        WriteStatusFile(True)
    End Sub

    ''' <summary>
    ''' Total time the job has been running
    ''' </summary>
    ''' <returns>Number of hours manager has been processing job</returns>
    ''' <remarks></remarks>
    Private Function GetRunTime() As Single

        Return CSng(Date.UtcNow.Subtract(TaskStartTime).TotalHours)

    End Function

    Public Sub DisposeMessageQueue()
        If Not m_MessageSender Is Nothing Then
            m_QueueLogger.Dispose()
            m_MessageSender.Dispose()
        End If

    End Sub

#End Region

End Class


