'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/2006
'
' Last modified 06/11/2009
'*********************************************************************************************************

Option Strict On

Public Interface IStatusFile

	'*********************************************************************************************************
	'Interface used by classes that create and update analysis status file
	'*********************************************************************************************************

#Region "Enums"
	'Status constants
	Enum EnumMgrStatus As Short
		STOPPED
		STOPPED_ERROR
		RUNNING
		DISABLED_LOCAL
		DISABLED_MC
	End Enum

	Enum EnumTaskStatus As Short
		STOPPED
		REQUESTING
		RUNNING
		CLOSING
		FAILED
		NO_TASK
	End Enum

	Enum EnumTaskStatusDetail As Short
		RETRIEVING_RESOURCES
		RUNNING_TOOL
		PACKAGING_RESULTS
		DELIVERING_RESULTS
		CLOSING
		NO_TASK
	End Enum
#End Region

#Region "Properties"
	Property FileNamePath() As String

	Property MgrName() As String

	Property MgrStatus() As EnumMgrStatus

	Property CpuUtilization() As Integer

	Property Tool() As String

	Property TaskStatus() As EnumTaskStatus

	Property Progress() As Single

	Property CurrentOperation() As String

	Property TaskStatusDetail() As EnumTaskStatusDetail

	Property JobNumber() As Integer

	Property JobStep() As Integer

	Property Dataset() As String

	Property MostRecentJobInfo() As String

	Property SpectrumCount() As Integer

	Property MessageQueueURI() As String

	Property MessageQueueTopic() As String

	Property LogToMsgQueue() As Boolean

#End Region

#Region "Methods"

    ''' <summary>
    ''' Returns the number of cores
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks>Not affected by hyperthreading, so a computer with two 4-core chips will report 8 cores</remarks>
    Function GetCoreCount() As Integer

    ''' <summary>
    ''' Returns the amount of free memory
    ''' </summary>
    ''' <returns>Amount of free memory, in MB</returns>
    Function GetFreeMemoryMB() As Single

    ''' <summary>
    ''' Return the ProcessID of the running process
    ''' </summary>
    ''' <returns></returns>
    ''' <remarks></remarks>
    Function GetProcessID() As Integer


    ''' <summary>
    ''' 
    ''' </summary>
    ''' <param name="ManagerIdleMessage"></param>
    ''' <param name="RecentErrorMessages"></param>
    ''' <param name="JobInfo"></param>
    ''' <param name="ForceLogToBrokerDB"></param>
    ''' <remarks></remarks>
    Sub UpdateClose(ByVal ManagerIdleMessage As String, _
      ByRef RecentErrorMessages() As String, _
      ByVal JobInfo As String, _
      ByVal ForceLogToBrokerDB As Boolean)


    ''' <summary>
    ''' Update the current status
    ''' </summary>
    ''' <param name="PercentComplete">Job completion percentage (value between 0 and 100)</param>
    ''' <remarks></remarks>
    Sub UpdateAndWrite(ByVal PercentComplete As Single)

    ''' <summary>
    ''' Update the current status
    ''' </summary>
    ''' <param name="eMgrStatus">Job status code</param>
    ''' <param name="eTaskStatus">Task status code</param>
    ''' <param name="eTaskStatusDetail">Detailed task status</param>
    ''' <param name="PercentComplete">Job completion percentage (value between 0 and 100)</param>
    ''' <remarks></remarks>
    Sub UpdateAndWrite(ByVal eMgrStatus As EnumMgrStatus,
                       ByVal eTaskStatus As EnumTaskStatus,
                       ByVal eTaskStatusDetail As EnumTaskStatusDetail,
                       ByVal PercentComplete As Single)

    ''' <summary>
    ''' Update the current status
    ''' </summary>
    ''' <param name="Status">Job status code</param>
    ''' <param name="PercentComplete">VJob completion percentage (value between 0 and 100)</param>
    ''' <param name="SpectrumCountTotal">Number of DTA files (i.e., spectra files); relevant for Sequest, X!Tandem, and Inspect</param>
    ''' <remarks></remarks>
    Sub UpdateAndWrite(ByVal Status As EnumTaskStatus,
                       ByVal PercentComplete As Single,
                       ByVal SpectrumCountTotal As Integer)

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
    Sub UpdateAndWrite(
      ByVal eMgrStatus As EnumMgrStatus, ByVal eTaskStatus As EnumTaskStatus, ByVal eTaskStatusDetail As EnumTaskStatusDetail, _
      ByVal PercentComplete As Single, ByVal DTACount As Integer, _
      ByVal MostRecentLogMessage As String, ByVal MostRecentErrorMessage As String, _
      ByVal RecentJobInfo As String, ByVal ForceLogToBrokerDB As Boolean)

    ''' <summary>
    ''' Logs to the status file that the manager is idle
    ''' </summary>
    ''' <remarks></remarks>
    Sub UpdateIdle()

    ''' <summary>
    ''' Logs to the status file that the manager is idle
    ''' </summary>
    ''' <param name="ManagerIdleMessage">Reason why the manager is idle (leave blank if unknown)</param>
    ''' <param name="ForceLogToBrokerDB">If true, then will force m_BrokerDBLogger to report the manager status to the database</param>
    ''' <remarks></remarks>
    Sub UpdateIdle(ByVal ManagerIdleMessage As String, ByVal ForceLogToBrokerDB As Boolean)

    ''' <summary>
    ''' Logs to the status file that the manager is idle
    ''' </summary>
    ''' <param name="ManagerIdleMessage">Reason why the manager is idle (leave blank if unknown)</param>
    ''' <param name="IdleErrorMessage">Error message explaining why the manager is idle</param>
    ''' <param name="RecentJobInfo">Information on the job that started most recently</param>
    ''' <param name="ForceLogToBrokerDB">If true, then will force m_BrokerDBLogger to report the manager status to the database</param>
    ''' <remarks></remarks>
    Sub UpdateIdle(ByVal ManagerIdleMessage As String, ByVal IdleErrorMessage As String, ByVal RecentJobInfo As String, ByVal ForceLogToBrokerDB As Boolean)

    ''' <summary>
    ''' Logs to the status file that the manager is idle
    ''' </summary>
    ''' <param name="ManagerIdleMessage">Reason why the manager is idle (leave blank if unknown)</param>
    ''' <param name="RecentErrorMessages">Recent error messages written to the log file (leave blank if unknown)</param>
    ''' <param name="RecentJobInfo">Information on the job that started most recently</param>
    ''' <param name="ForceLogToBrokerDB">If true, then will force m_BrokerDBLogger to report the manager status to the database</param>
    ''' <remarks></remarks>
    Sub UpdateIdle(ByVal ManagerIdleMessage As String, ByRef RecentErrorMessages() As String, ByVal RecentJobInfo As String, ByVal ForceLogToBrokerDB As Boolean)


    ''' <summary>
    ''' Logs to the status file that the manager is disabled (either in the manager control DB or via the local AnalysisManagerProg.exe.config file)
    ''' </summary>
    ''' <remarks></remarks>
    Sub UpdateDisabled(ByVal ManagerStatus As EnumMgrStatus)

    ''' <summary>
    ''' Logs to the status file that the manager is disabled (either in the manager control DB or via the local AnalysisManagerProg.exe.config file)
    ''' </summary>
    ''' <param name="ManagerDisableMessage">Description of why the manager is disabled (leave blank if unknown)</param>
    ''' <remarks></remarks>
    Sub UpdateDisabled(ByVal ManagerStatus As EnumMgrStatus, ByVal ManagerDisableMessage As String)

    ''' <summary>
    ''' Logs to the status file that the manager is disabled (either in the manager control DB or via the local AnalysisManagerProg.exe.config file)
    ''' </summary>
    ''' <param name="ManagerStatus">Should be EnumMgrStatus.DISABLED_LOCAL or EnumMgrStatus.DISABLED_MC</param>
    ''' <param name="ManagerDisableMessage">Description of why the manager is disabled (leave blank if unknown)</param>
    ''' <param name="RecentErrorMessages">Recent error messages written to the log file (leave blank if unknown)</param>
    ''' <param name="RecentJobInfo">Information on the job that started most recently</param>
    ''' <remarks></remarks>
    Sub UpdateDisabled(ByVal ManagerStatus As EnumMgrStatus, ByVal ManagerDisableMessage As String, ByRef RecentErrorMessages() As String, ByVal RecentJobInfo As String)

    ''' <summary>
    ''' Logs to the status file that a flag file exists, indicating that the manager did not exit cleanly on a previous run
    ''' </summary>
    ''' <remarks></remarks>
    Sub UpdateFlagFileExists()

    ''' <summary>
    ''' Logs to the status file that a flag file exists, indicating that the manager did not exit cleanly on a previous run
    ''' </summary>
    ''' <param name="RecentErrorMessages">Recent error messages written to the log file (leave blank if unknown)</param>
    ''' <param name="RecentJobInfo">Information on the job that started most recently</param>
    ''' <remarks></remarks>
    Sub UpdateFlagFileExists(ByRef RecentErrorMessages() As String, ByVal RecentJobInfo As String)

    ''' <summary>
    ''' Writes out a new status file, indicating that the manager is still alive
    ''' </summary>
    ''' <remarks></remarks>
    Sub WriteStatusFile()

    ''' <summary>
    ''' Writes the status file
    ''' </summary>
    ''' <param name="ForceLogToBrokerDB">If true, then will force m_BrokerDBLogger to report the manager status to the database</param>
    ''' <remarks></remarks>
    Sub WriteStatusFile(ByVal ForceLogToBrokerDB As Boolean)

#End Region

End Interface


