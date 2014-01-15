'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/19/2007
'
' Last modified 06/11/2009 JDS - Added logging using log4net
'*********************************************************************************************************

Option Strict On

Imports System.IO
Imports System.Text.RegularExpressions
Imports System.Threading
Imports AnalysisManagerBase

Public Class clsMainProcess

	'*********************************************************************************************************
	'Master processing class for analysis manager
	'*********************************************************************************************************

#Region "Constants"
	' These constants are used to create the Windows Event log (aka the EmergencyLog) that this program rights to
	'  when the manager is disabled or cannot make an entry in the log file
	Private Const CUSTOM_LOG_SOURCE_NAME As String = "Analysis Manager"
	Public Const CUSTOM_LOG_NAME As String = "DMS_AnalysisMgr"
	Private Const MAX_ERROR_COUNT As Integer = 6

	Private Const DECON2LS_FATAL_REMOTING_ERROR As String = "Fatal remoting error"
	Private Const DECON2LS_CORRUPTED_MEMORY_ERROR As String = "Corrupted memory error"
	Private Const DECON2LS_TCP_ALREADY_REGISTERED_ERROR As String = "channel 'tcp' is already registered"
#End Region

#Region "Module variables"
	Private m_MainProcess As clsMainProcess
	Private m_MgrSettings As IMgrParams				' clsAnalysisMgrSettings
	Private m_MgrErrorCleanup As clsCleanupMgrErrors

	Private ReadOnly m_MgrFolderPath As String
	Private m_WorkDirPath As String
	Private m_MgrName As String = "??"

	Private m_AnalysisTask As IJobParams			' clsAnalysisJob
	Private m_PluginLoader As clsPluginLoader
	Private m_SummaryFile As clsSummaryFile

	Private WithEvents m_FileWatcher As FileSystemWatcher
	Private m_ConfigChanged As Boolean
	Private m_DebugLevel As Integer
	Private m_Resource As IAnalysisResources
	Private m_ToolRunner As IToolRunner
	Private m_StatusTools As clsStatusFile
	Private m_NeedToAbortProcessing As Boolean
	Private m_MostRecentJobInfo As String

	Private m_MostRecentErrorMessage As String = String.Empty

	Private m_TraceMode As Boolean

	Declare Auto Function GetDiskFreeSpaceEx Lib "kernel32.dll" ( _
	   ByVal lpRootPathName As String, _
	   ByRef lpFreeBytesAvailable As Long, _
	   ByRef lpTotalNumberOfBytes As Long, _
	   ByRef lpTotalNumberOfFreeBytes As Long) As Integer

#End Region

#Region "Properties"
	Public Property TraceMode As Boolean
		Get
			Return m_TraceMode
		End Get
		Set(value As Boolean)
			m_TraceMode = value
		End Set
	End Property

#End Region

#Region "Methods"
	''' <summary>
	''' Starts program execution
	''' </summary>
	''' <returns>0 if no error; error code if an error</returns>
	''' <remarks></remarks>
	Public Function Main() As Integer

		Dim ErrMsg As String

		Try

			If IsNothing(m_MainProcess) Then
				If Me.TraceMode Then ShowTraceMessage("Instantiating m_MainProcess in clsMainProcess")

				m_MainProcess = New clsMainProcess(Me.TraceMode)
				If Not m_MainProcess.InitMgr Then
					If Me.TraceMode Then ShowTraceMessage("m_MainProcess.InitMgr returned false; aborting")
					Exit Function
				End If

			End If

			If Me.TraceMode Then ShowTraceMessage("Call m_MainProcess.DoAnalysis")
			m_MainProcess.DoAnalysis()

			If Me.TraceMode Then ShowTraceMessage("Exiting clsMainProcess.Main with error code = 0")
			Return 0

		Catch Err As Exception
			'Report any exceptions not handled at a lower level to the system application log
			ErrMsg = "Critical exception starting application: " & Err.Message & "; " & clsGlobal.GetExceptionStackTrace(Err)
			If Me.TraceMode Then ShowTraceMessage(ErrMsg)
			PostToEventLog(ErrMsg)
			If Me.TraceMode Then ShowTraceMessage("Exiting clsMainProcess.Main with error code = 1")
			Return 1
		End Try

	End Function

	''' <summary>
	''' Constructor
	''' </summary>	
	Public Sub New(blnTraceModeEnabled As Boolean)
		Me.TraceMode = blnTraceModeEnabled
		m_ConfigChanged = False
		m_DebugLevel = 0
		m_NeedToAbortProcessing = False
		m_MostRecentJobInfo = String.Empty

		Dim fiMgr As FileInfo = New FileInfo(Application.ExecutablePath)
		m_MgrFolderPath = fiMgr.DirectoryName

	End Sub

	''' <summary>
	''' Initializes the manager settings
	''' </summary>
	''' <returns>TRUE for success, FALSE for failure</returns>
	''' <remarks></remarks>
	Private Function InitMgr() As Boolean

		' Get settings from config file
		Dim lstMgrSettings As Dictionary(Of String, String)

		Try
			If Me.TraceMode Then ShowTraceMessage("Reading application config file")
			lstMgrSettings = LoadMgrSettingsFromFile()

			' Get the manager settings
			' If you get an exception here while debugging in Visual Studio, then be sure 
			'   that "UsingDefaults" is set to False in CaptureTaskManager.exe.config               
			Try
				If Me.TraceMode Then ShowTraceMessage("Instantiating clsAnalysisMgrSettings")
				m_MgrSettings = New clsAnalysisMgrSettings(CUSTOM_LOG_SOURCE_NAME, CUSTOM_LOG_NAME, lstMgrSettings, m_MgrFolderPath)
			Catch ex As Exception
				' Failures are logged by clsMgrSettings to application event logs;
				'  this includes MgrActive_Local = False
				'  
				' If the DMS_AnalysisMgr application log does not exist yet, the SysLogger will create it
				' However, in order to do that, the program needs to be running from an elevated (administrative level) command prompt
				' Thus, it is advisable to run this program once from an elevated command prompt while MgrActive_Local is set to false

				Console.WriteLine()
				Console.WriteLine("===============================================================")
				Console.WriteLine("Exception instantiating clsAnalysisMgrSettings: " & ex.Message)
				Console.WriteLine("===============================================================")
				Console.WriteLine()
				Console.WriteLine("You may need to start this application once from an elevated (administrative level) command prompt using the /EL switch so that it can create the " & CUSTOM_LOG_NAME & " application log")
				Console.WriteLine()
				Thread.Sleep(500)

				Return False
			End Try


		Catch ex As Exception
			Console.WriteLine()
			Console.WriteLine("===============================================================")
			Console.WriteLine("Exception loading settings from AnalysisManagerProg.exe.config: " & ex.Message)
			Console.WriteLine("===============================================================")
			Console.WriteLine()
			Thread.Sleep(500)
			Return False
		End Try

		m_MgrName = m_MgrSettings.GetParam("MgrName")
		If Me.TraceMode Then ShowTraceMessage("Manager name is " & m_MgrName)

		' Delete any temporary files that may be left in the app directory
		RemoveTempFiles()

		' Setup the logger
		Dim LogFileName As String = m_MgrSettings.GetParam("logfilename")

		' Make the initial log entry
		If Me.TraceMode Then ShowTraceMessage("Initializing log file " & LogFileName)
		clsLogTools.ChangeLogFileName(LogFileName)

		Dim MyMsg As String = "=== Started Analysis Manager V" & Application.ProductVersion & " ===== "
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, MyMsg)

		' Setup a file watcher for the config file
		m_FileWatcher = New FileSystemWatcher
		With m_FileWatcher
			.BeginInit()
			.Path = m_MgrFolderPath
			.IncludeSubdirectories = False
			.Filter = m_MgrSettings.GetParam("configfilename")
			.NotifyFilter = NotifyFilters.LastWrite Or NotifyFilters.Size
			.EndInit()
			.EnableRaisingEvents = True
		End With

		' Get the debug level
		m_DebugLevel = CInt(m_MgrSettings.GetParam("debuglevel"))

		' Setup the tool for getting tasks
		If Me.TraceMode Then ShowTraceMessage("Instantiate m_AnalysisTask as new clsAnalysisJob")
		m_AnalysisTask = New clsAnalysisJob(m_MgrSettings, m_DebugLevel)

		m_WorkDirPath = m_MgrSettings.GetParam("workdir")

		' Setup the manager cleanup class
		If Me.TraceMode Then ShowTraceMessage("Setup the manager cleanup class")
		m_MgrErrorCleanup = New clsCleanupMgrErrors( _
		   m_MgrSettings.GetParam("MgrCnfgDbConnectStr"), _
		   m_MgrName, _
		   m_MgrFolderPath, _
		   m_WorkDirPath)

		If Me.TraceMode Then ShowTraceMessage("Initialize the Summary file")
		m_SummaryFile = New clsSummaryFile()
		m_SummaryFile.Clear()

		If Me.TraceMode Then ShowTraceMessage("Initialize the Plugin Loader")
		m_PluginLoader = New clsPluginLoader(m_SummaryFile, m_MgrFolderPath)

		'Everything worked
		Return True

	End Function

	''' <summary>
	''' Loop to perform all analysis jobs
	''' </summary>
	''' <remarks></remarks>
	Public Sub DoAnalysis()
		If Me.TraceMode Then ShowTraceMessage("Entering clsMainProcess.DoAnalysis")

		Dim LoopCount As Integer = 0
		Dim MaxLoopCount As Integer
		Dim TasksStartedCount As Integer = 0
		Dim blnErrorDeletingFilesFlagFile As Boolean

		Dim strMessage As String
		Dim dtLastConfigDBUpdate As DateTime = DateTime.UtcNow

		Dim blnRequestJobs As Boolean
		Dim blnOneTaskStarted As Boolean
		Dim blnOneTaskPerformed As Boolean

		Dim intErrorCount As Integer = 0
		Dim intSuccessiveDeadLockCount As Integer = 0

		Try
			If Me.TraceMode Then ShowTraceMessage("Entering clsMainProcess.DoAnalysis Try/Catch block")

			MaxLoopCount = CInt(m_MgrSettings.GetParam("maxrepetitions"))
			blnRequestJobs = True
			blnOneTaskStarted = False
			blnOneTaskPerformed = False

			InitStatusTools()

			While (LoopCount < MaxLoopCount) And blnRequestJobs

				UpdateStatusIdle("No analysis jobs found")

				' Check for configuration change
				' This variable will be true if the CaptureTaskManager.exe.config file has been updated
				If m_ConfigChanged Then
					'Local config file has changed
					m_ConfigChanged = False

					If Me.TraceMode Then ShowTraceMessage("Reloading manager settings since config file has changed")

					If Not ReloadManagerSettings() Then
						Exit Sub
					End If
					m_FileWatcher.EnableRaisingEvents = True
				Else

					' Reload the manager control DB settings in case they have changed
					' However, only reload every 2 minutes
					If Not UpdateManagerSettings(dtLastConfigDBUpdate, 2) Then
						' Error retrieving settings from the manager control DB
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, "===== Closing Analysis Manager =====")
						Exit Sub
					End If

				End If

				'Check to see if manager is still active
				Dim MgrActive As Boolean = m_MgrSettings.GetParam("mgractive", False)
				Dim MgrActiveLocal As Boolean = m_MgrSettings.GetParam("mgractive_local", False)
				Dim strManagerDisableReason As String
				If Not (MgrActive And MgrActiveLocal) Then
					If Not MgrActiveLocal Then
						strManagerDisableReason = "Disabled locally via AnalysisManagerProg.exe.config"
						UpdateStatusDisabled(IStatusFile.EnumMgrStatus.DISABLED_LOCAL, strManagerDisableReason)
					Else
						strManagerDisableReason = "Disabled in Manager Control DB"
						UpdateStatusDisabled(IStatusFile.EnumMgrStatus.DISABLED_MC, strManagerDisableReason)
					End If

					If Me.TraceMode Then ShowTraceMessage("Manager inactive: " & strManagerDisableReason)
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Manager inactive: " & strManagerDisableReason)
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "===== Closing Analysis Manager =====")
					Exit Sub
				End If

				Dim MgrUpdateRequired As Boolean = m_MgrSettings.GetParam("ManagerUpdateRequired", False)
				If MgrUpdateRequired Then
					If Me.TraceMode Then ShowTraceMessage("Manager update is required, closing manager")
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Manager update is required")
					m_MgrSettings.AckManagerUpdateRequired()
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "===== Closing Analysis Manager =====")
					UpdateStatusIdle("Manager update is required")
					Exit Sub
				End If

				If m_MgrErrorCleanup.DetectErrorDeletingFilesFlagFile() Then
					'Delete the Error Deleting status flag file first, so next time through this step is skipped
					m_MgrErrorCleanup.DeleteErrorDeletingFilesFlagFile()

					'There was a problem deleting non result files with the last job.  Attempt to delete files again
					If Not m_MgrErrorCleanup.CleanWorkDir() Then
						If blnOneTaskStarted Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error cleaning working directory, job " & m_AnalysisTask.GetParam("StepParameters", "Job") & "; see folder " & m_WorkDirPath)
							m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "Error cleaning working directory")
						Else
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error cleaning working directory; see folder " & m_WorkDirPath)
						End If
						m_MgrErrorCleanup.CreateStatusFlagFile()
						UpdateStatusFlagFileExists()
						Exit Sub
					End If
					'successful delete of files in working directory, so delete the status flag file
					m_MgrErrorCleanup.DeleteStatusFlagFile(m_DebugLevel)
				End If

				'Verify that an error hasn't left the the system in an odd state
				If StatusFlagFileError() Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Flag file exists - unable to perform any further analysis jobs")
					UpdateStatusFlagFileExists()
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "===== Closing Analysis Manager =====")
					Exit Sub
				End If

				'Check to see if an excessive number of errors have occurred
				If intErrorCount > MAX_ERROR_COUNT Then
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Excessive task failures; disabling manager via flag file")

					' Note: We previously called DisableManagerLocally() to update AnalysisManager.config.exe
					' We now create a flag file instead
					' This gives the manager a chance to auto-cleanup things if ManagerErrorCleanupMode is >= 1

					m_MgrErrorCleanup.CreateStatusFlagFile()
					UpdateStatusFlagFileExists()

					Exit While
				End If

				'Verify working directory properly specified and empty
				If Not ValidateWorkingDir() Then
					If blnOneTaskStarted Then
						' Working directory problem due to the most recently processed job
						' Create ErrorDeletingFiles file and exit the program
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Working directory problem, creating " & clsCleanupMgrErrors.ERROR_DELETING_FILES_FILENAME & "; see folder " & m_WorkDirPath)
						m_MgrErrorCleanup.CreateErrorDeletingFilesFlagFile()
						UpdateStatusIdle("Working directory not empty")
					Else
						' Working directory problem, so create flag file and exit
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Working directory problem, disabling manager via flag file; see folder " & m_WorkDirPath)
						m_MgrErrorCleanup.CreateStatusFlagFile()
						UpdateStatusFlagFileExists()
					End If
					Exit While
				End If

				'Get an analysis job, if any are available
				Dim TaskReturn As clsAnalysisJob.RequestTaskResult
				TaskReturn = m_AnalysisTask.RequestTask()
				Select Case TaskReturn
					Case clsDBTask.RequestTaskResult.NoTaskFound
						'No tasks found
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, "No analysis jobs found")
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogSystem, clsLogTools.LogLevels.INFO, "No analysis jobs found")
						blnRequestJobs = False
						intErrorCount = 0
						UpdateStatusIdle("No analysis jobs found")

					Case clsDBTask.RequestTaskResult.ResultError
						'There was a problem getting the task; errors were logged by RequestTaskResult
						intErrorCount += 1

					Case clsDBTask.RequestTaskResult.TaskFound
						blnRequestJobs = True
						TasksStartedCount += 1
						intSuccessiveDeadLockCount = 0

						Try
							blnOneTaskStarted = True
							If DoAnalysisJob() Then
								' Task succeeded; reset the sequential job failure counter
								intErrorCount = 0
								blnOneTaskPerformed = True
							Else
								'Something went wrong; errors were logged by DoAnalysisJob
								If m_MostRecentErrorMessage.Contains("None of the spectra are centroided") Then
									' Job failed, but this was not a manager error
									' Do not increment the error count
								Else
									intErrorCount += 1
								End If
							End If

						Catch ex As Exception
							' Something went wrong; errors likely were not logged by DoAnalysisJob

							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMainProcess.DoAnalysis(), Exception thrown by DoAnalysisJob, " & _
							 ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex))
							m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysis(): " & ex.Message, m_MostRecentJobInfo, True)

							' Set the job state to failed
							m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "Exception thrown by DoAnalysisJob")

							intErrorCount += 1
							m_NeedToAbortProcessing = True

						End Try

					Case clsDBTask.RequestTaskResult.TooManyRetries
						'There were too many retries calling the stored procedure; errors were logged by RequestTaskResult
						' Bump up LoopCount to the maximum to exit the loop
						UpdateStatusIdle("Excessive retries requesting task")
						LoopCount = MaxLoopCount

					Case clsDBTask.RequestTaskResult.Deadlock
						' A deadlock error occured
						' Query the DB again, but only if we have not had 3 deadlock results in a row
						intSuccessiveDeadLockCount += 1
						If intSuccessiveDeadLockCount >= 3 Then
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Deadlock encountered " & intSuccessiveDeadLockCount.ToString() & " times in a row when requesting a new task; exiting")
							blnRequestJobs = False
						End If

					Case Else
						'Shouldn't ever get here
						Dim MyErr As String = "clsMainProcess.DoAnalysis; Invalid request result: "
						MyErr &= CInt(TaskReturn).ToString
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, MyErr)
						Exit Sub
				End Select

				If NeedToAbortProcessing() Then Exit While
				LoopCount += 1

				'if the only problem was deleting non result files, we want to stop the manager
				If m_MgrErrorCleanup.DetectErrorDeletingFilesFlagFile() Then
					blnErrorDeletingFilesFlagFile = True
					LoopCount = MaxLoopCount
				End If

			End While

			If LoopCount >= MaxLoopCount Then
				If blnErrorDeletingFilesFlagFile Then
					If TasksStartedCount > 0 Then
						strMessage = "Error deleting file with an open file handle; closing manager.  Jobs processed: " & TasksStartedCount.ToString
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strMessage)
					End If
				Else
					If TasksStartedCount > 0 Then
						strMessage = "Maximum number of jobs to analyze has been reached: " & TasksStartedCount.ToString & " job"
						If TasksStartedCount <> 1 Then strMessage &= "s"
						strMessage &= "; closing manager"
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, strMessage)
					End If
				End If
			End If

			If blnOneTaskPerformed Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, "Analysis complete for all available jobs")
			End If

			UpdateClose("Closing manager.")
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "===== Closing Analysis Manager =====")
		Catch Err As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMainProcess.DoAnalysis(), Error encountered, " & _
			 Err.Message & "; " & clsGlobal.GetExceptionStackTrace(Err))
			m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysis(): " & Err.Message, m_MostRecentJobInfo, True)

		Finally
			If Not m_StatusTools Is Nothing Then
				If Me.TraceMode Then ShowTraceMessage("Disposing message queue via m_StatusTools.DisposeMessageQueue")
				m_StatusTools.DisposeMessageQueue()
			End If
		End Try

	End Sub

	Private Function DoAnalysisJob() As Boolean

		Dim eToolRunnerResult As IJobParams.CloseOutType
		Dim JobNum As Integer = m_AnalysisTask.GetJobParameter("StepParameters", "Job", 0)
		Dim StepNum As Integer = m_AnalysisTask.GetJobParameter("StepParameters", "Step", 0)
		Dim Dataset As String = m_AnalysisTask.GetParam("JobParameters", "DatasetNum")
		Dim JobToolDescription As String = m_AnalysisTask.GetCurrentJobToolDescription

		Dim blnRunToolError As Boolean = False

		'Initialize summary and status files
		m_SummaryFile.Clear()

		If m_StatusTools Is Nothing Then
			InitStatusTools()
		End If


		' Update the cached most recent job info
		m_MostRecentJobInfo = ConstructMostRecentJobInfoText(DateTime.Now.ToString(), JobNum, Dataset, JobToolDescription)

		With m_StatusTools
			.TaskStartTime = DateTime.UtcNow
			.Dataset = Dataset
			.JobNumber = JobNum
			.JobStep = StepNum
			.Tool = JobToolDescription
			.MgrName = m_MgrName
			.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RETRIEVING_RESOURCES, 0, 0, "", "", m_MostRecentJobInfo, True)
		End With

		' Note: The format of the following text is important; be careful about changing it
		' In particular, function DetermineRecentErrorMessages in clsMainProcess looks for log entries
		'   matching RegEx: "^([^,]+),.+Started analysis job (\d+), Dataset (.+), Tool (.+), Normal"
		clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, m_MgrName & ": Started analysis job " & JobNum & ", Dataset " & Dataset & ", Tool " & JobToolDescription)

		If m_DebugLevel >= 2 Then
			' Log the debug level value whenever the debug level is 2 or higher
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Debug level is " & m_DebugLevel.ToString)
		End If

		'Create an object to manage the job resources
		If Not SetResourceObject() Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, m_MgrName & ": Unable to SetResourceObject, job " & JobNum & ", Dataset " & Dataset)
			m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "Unable to set resource object")
			m_MgrErrorCleanup.CleanWorkDir()
			UpdateStatusIdle("Error encountered: Unable to set resource object")
			Return False
		End If

		'Create an object to run the analysis tool
		If Not SetToolRunnerObject() Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, m_MgrName & ": Unable to SetToolRunnerObject, job " & JobNum & ", Dataset " & Dataset)
			m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "Unable to set tool runner object")
			m_MgrErrorCleanup.CleanWorkDir()
			UpdateStatusIdle("Error encountered: Unable to set tool runner object")
			Return False
		End If

		If NeedToAbortProcessing() Then
			m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "Processing aborted")
			m_MgrErrorCleanup.CleanWorkDir()
			UpdateStatusIdle("Processing aborted")
			Return False
		End If

		' Make sure we have enough free space on the drive with the working directory and on the drive with the transfer folder
		If Not ValidateFreeDiskSpace(m_MostRecentErrorMessage) Then
			If String.IsNullOrEmpty(m_MostRecentErrorMessage) Then
				m_MostRecentErrorMessage = "Insufficient free space (location undefined)"
			End If
			m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, m_MostRecentErrorMessage)
			m_MgrErrorCleanup.CleanWorkDir()
			UpdateStatusIdle("Processing aborted")
			Return False
		End If

		'Retrieve files required for the job
		m_MgrErrorCleanup.CreateStatusFlagFile()
		Try
			eToolRunnerResult = m_Resource.GetResources()
			If Not eToolRunnerResult = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				m_MostRecentErrorMessage = "GetResources returned result: " & eToolRunnerResult.ToString
				If Not m_Resource.Message Is Nothing Then
					m_MostRecentErrorMessage &= "; " & m_Resource.Message
				End If

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, m_MgrName & ": " & m_MostRecentErrorMessage & ", Job " & JobNum & ", Dataset " & Dataset)
				m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, m_Resource.Message)

				m_MgrErrorCleanup.CleanWorkDir()
				UpdateStatusIdle("Error encountered: " & m_MostRecentErrorMessage)
				m_MgrErrorCleanup.DeleteStatusFlagFile(m_DebugLevel)
				Return False
			End If
		Catch Err As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMainProcess.DoAnalysisJob(), Getting resources, " & _
			 Err.Message & "; " & clsGlobal.GetExceptionStackTrace(Err))

			m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "Exception getting resources")

			If m_MgrErrorCleanup.CleanWorkDir() Then
				m_MgrErrorCleanup.DeleteStatusFlagFile(m_DebugLevel)
			Else
				m_MgrErrorCleanup.CreateErrorDeletingFilesFlagFile()
			End If

			m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysisJob(): " & Err.Message, m_MostRecentJobInfo, True)
			Return False
		End Try

		'Run the job
		m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, 0)
		Try
			eToolRunnerResult = m_ToolRunner.RunTool()
			If eToolRunnerResult <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
				m_MostRecentErrorMessage = m_ToolRunner.Message

				If String.IsNullOrEmpty(m_MostRecentErrorMessage) Then
					m_MostRecentErrorMessage = "Unknown ToolRunner Error"
				End If

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_MgrName & ": " & m_MostRecentErrorMessage & ", Job " & JobNum & ", Dataset " & Dataset)
				m_AnalysisTask.CloseTask(eToolRunnerResult, m_MostRecentErrorMessage, m_ToolRunner.EvalCode, m_ToolRunner.EvalMessage)

				Try
					If m_MostRecentErrorMessage.Contains(DECON2LS_FATAL_REMOTING_ERROR) OrElse _
					   m_MostRecentErrorMessage.Contains(DECON2LS_CORRUPTED_MEMORY_ERROR) Then
						m_NeedToAbortProcessing = True
					End If

				Catch ex As Exception
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMainProcess.DoAnalysisJob(), Exception examining MostRecentErrorMessage", ex)
				End Try

				If eToolRunnerResult = IJobParams.CloseOutType.CLOSEOUT_ERROR_ZIPPING_FILE Then
					m_NeedToAbortProcessing = True
				End If

				If m_NeedToAbortProcessing AndAlso m_MostRecentErrorMessage.StartsWith(clsAnalysisToolRunnerBase.PVM_RESET_ERROR_MESSAGE) Then
					DisableManagerLocally()
				End If

				blnRunToolError = True
			End If

			If m_ToolRunner.NeedToAbortProcessing Then
				m_NeedToAbortProcessing = True
				m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, m_MostRecentErrorMessage, m_ToolRunner.EvalCode, m_ToolRunner.EvalMessage)
			End If

		Catch Err As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMainProcess.DoAnalysisJob(), running tool, " & Err.Message & "; " & clsGlobal.GetExceptionStackTrace(Err))

			If Err.Message.Contains(DECON2LS_TCP_ALREADY_REGISTERED_ERROR) Then
				m_NeedToAbortProcessing = True
			End If

			m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "Exception running tool", m_ToolRunner.EvalCode, m_ToolRunner.EvalMessage)

			blnRunToolError = True
		End Try

		If blnRunToolError Then
			' Note: the above code should have already called m_AnalysisTask.CloseTask()

			Try
				If m_MgrErrorCleanup.CleanWorkDir() Then
					m_MgrErrorCleanup.DeleteStatusFlagFile(m_DebugLevel)
				Else
					m_MgrErrorCleanup.CreateErrorDeletingFilesFlagFile()
				End If

				If eToolRunnerResult = IJobParams.CloseOutType.CLOSEOUT_NO_DTA_FILES AndAlso _
				   m_AnalysisTask.GetParam("StepTool").ToLower = "sequest" Then
					' This was a Sequest job, but no .DTA files were found
					' We return True here because we don't want this problem to be counted as a manager failure
					Return True
				Else
					Return False
				End If

			Catch Err As Exception
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMainProcess.DoAnalysisJob(), cleaning up after RunTool error," & _
				  Err.Message & "; " & clsGlobal.GetExceptionStackTrace(Err))
				m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysisJob(): " & Err.Message, m_MostRecentJobInfo, True)
				Return False
			End Try

		End If

		'Close out the job
		m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.CLOSING, IStatusFile.EnumTaskStatusDetail.CLOSING, 100)
		Try
			'Close out the job as a success
			m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_SUCCESS, String.Empty, m_ToolRunner.EvalCode, m_ToolRunner.EvalMessage)
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.INFO, m_MgrName & ": Completed job " & JobNum)

			UpdateStatusIdle("Completed job " & JobNum & ", step " & StepNum)

		Catch err As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMainProcess.DoAnalysisJob(), Close task after normal run," & _
			 err.Message & "; " & clsGlobal.GetExceptionStackTrace(err))
			m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysisJob(): " & err.Message, m_MostRecentJobInfo, True)
			Return False
		End Try

		Try
			'If success was reported check to see if there was an error deleting non result files
			If m_MgrErrorCleanup.DetectErrorDeletingFilesFlagFile() Then
				'If there was a problem deleting non result files, return success and let the manager try to delete the files one more time on the next start up
				' However, wait another 5 seconds before continuing
				PRISM.Processes.clsProgRunner.GarbageCollectNow()
				Thread.Sleep(5000)

				Return True
			Else
				'Clean the working directory
				Try
					If Not m_MgrErrorCleanup.CleanWorkDir() Then
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error cleaning working directory, job " & m_AnalysisTask.GetParam("StepParameters", "Job"))
						m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "Error cleaning working directory")
						m_MgrErrorCleanup.CreateErrorDeletingFilesFlagFile()
						Return False
					End If
				Catch Err As Exception
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMainProcess.DoAnalysisJob(), Clean work directory after normal run," & _
					   Err.Message & "; " & clsGlobal.GetExceptionStackTrace(Err))
					m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysisJob(): " & Err.Message, m_MostRecentJobInfo, True)
					Return False
				End Try

				'Delete the status flag file
				m_MgrErrorCleanup.DeleteStatusFlagFile(m_DebugLevel)

				' Note that we do not need to call m_StatusTools.UpdateIdle() here since 
				' we called UpdateStatusIdle() just after m_AnalysisTask.CloseTask above

				Return True
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsMainProcess.DoAnalysisJob(), " & _
			  ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex))
			m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysisJob(): " & ex.Message, m_MostRecentJobInfo, True)
			Return False
		End Try

	End Function

	''' <summary>
	''' Constructs a description of the given job using the job number, step tool name, and dataset name
	''' </summary>
	''' <param name="JobStartTimeStamp">Time job started</param>
	''' <param name="Job">Job name</param>
	''' <param name="Dataset">Dataset name</param>
	''' <param name="ToolName">Tool name (or step tool name)</param>
	''' <returns>Info string, similar to: Job 375797; DataExtractor (XTandem), Step 4; QC_Shew_09_01_b_pt5_25Mar09_Griffin_09-02-03; 3/26/2009 3:17:57 AM</returns>
	''' <remarks></remarks>
	Protected Function ConstructMostRecentJobInfoText(ByVal JobStartTimeStamp As String, ByVal Job As Integer, ByVal Dataset As String, ByVal ToolName As String) As String

		Try
			If JobStartTimeStamp Is Nothing Then JobStartTimeStamp = String.Empty
			If ToolName Is Nothing Then ToolName = "??"
			If Dataset Is Nothing Then Dataset = "??"

			Return "Job " & Job.ToString & "; " & ToolName & "; " & Dataset & "; " & JobStartTimeStamp
		Catch ex As Exception
			' Error combining the terms; return an empty string
			Return String.Empty
		End Try

	End Function

	Public Shared Sub CreateAnalysisManagerEventLog()
		Dim blnSuccess As Boolean
		blnSuccess = CreateAnalysisManagerEventLog(CUSTOM_LOG_SOURCE_NAME, CUSTOM_LOG_NAME)

		If blnSuccess Then
			Console.WriteLine()
			Console.WriteLine("Windows Event Log '" & CUSTOM_LOG_NAME & "' has been validated for source '" & CUSTOM_LOG_SOURCE_NAME & "'")
			Console.WriteLine()
		End If
	End Sub

	Protected Shared Function CreateAnalysisManagerEventLog(ByVal SourceName As String, ByVal LogName As String) As Boolean

		Try
			If String.IsNullOrEmpty(SourceName) Then
				Console.WriteLine("Error creating the Windows Event Log: SourceName cannot be blank")
				Return False
			End If

			If String.IsNullOrEmpty(LogName) Then
				Console.WriteLine("Error creating the Windows Event Log: LogName cannot be blank")
				Return False
			End If

			If Not EventLog.SourceExists(SourceName) Then
				Console.WriteLine("Creating Windows Event Log " & LogName & " for source " & SourceName)
				Dim SourceData As EventSourceCreationData = New EventSourceCreationData(SourceName, LogName)
				EventLog.CreateEventSource(SourceData)
			End If

			' Create custom event logging object and update it's configuration
			Dim ELog As New EventLog
			ELog.Log = LogName
			ELog.Source = SourceName

			Try
				ELog.MaximumKilobytes = 1024
			Catch ex As Exception
				Console.WriteLine("Warning: unable to update the maximum log size to 1024 KB: " & ControlChars.NewLine & "  " & ex.Message)
			End Try

			Try
				ELog.ModifyOverflowPolicy(OverflowAction.OverwriteAsNeeded, 90)
			Catch ex As Exception
				Console.WriteLine("Warning: unable to update the overflow policy to keep events for 90 days and overwrite as needed: " & ControlChars.NewLine & "  " & ex.Message)
			End Try

		Catch ex As Exception
			Console.WriteLine("Exception creating the Windows Event Log named '" & LogName & "' for source '" & SourceName & "': " & ex.Message)
			Return False
		End Try

		Return True

	End Function

	''' <summary>
	''' Given a log file with a name like AnalysisMgr_03-25-2009.txt, returns the log file name for the previous day
	''' </summary>
	''' <param name="strLogFilePath"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function DecrementLogFilePath(ByVal strLogFilePath As String) As String

		Dim reLogFileName As Regex
		Dim objMatch As Match

		Dim intYear As Integer
		Dim intMonth As Integer
		Dim intDay As Integer
		Dim strPreviousLogFilePath As String = String.Empty

		Try
			reLogFileName = New Regex("(.+_)(\d+)-(\d+)-(\d+).\S+", RegexOptions.Compiled Or RegexOptions.IgnoreCase)

			objMatch = reLogFileName.Match(strLogFilePath)

			If objMatch.Success AndAlso objMatch.Groups.Count >= 4 Then
				intMonth = CInt(objMatch.Groups(2).Value)
				intDay = CInt(objMatch.Groups(3).Value)
				intYear = CInt(objMatch.Groups(4).Value)

				Dim dtCurrentDate As DateTime
				Dim dtNewDate As DateTime

				dtCurrentDate = DateTime.Parse(intYear & "-" & intMonth & "-" & intDay)
				dtNewDate = dtCurrentDate.Subtract(New TimeSpan(1, 0, 0, 0))

				strPreviousLogFilePath = objMatch.Groups(1).Value & dtNewDate.ToString("MM-dd-yyyy") & Path.GetExtension(strLogFilePath)
			End If

		Catch ex As Exception
			Console.WriteLine("Error in DecrementLogFilePath: " & ex.Message)
		End Try

		Return strPreviousLogFilePath

	End Function

	''' <summary>
	''' Parses the log files for this manager to determine the recent error messages, returning up to intErrorMessageCountToReturn of them
	''' Will use objLogger to determine the most recent log file
	''' Also examines the message info stored in objLogger
	''' Lastly, if strMostRecentJobInfo is empty, then will update it with info on the most recent job started
	''' </summary>
	''' <param name="intErrorMessageCountToReturn">Maximum number of error messages to return</param>
	''' <param name="strMostRecentJobInfo">Info on the most recent job started by this manager</param>
	''' <returns></returns>
	''' <remarks></remarks>
	Public Function DetermineRecentErrorMessages(ByVal intErrorMessageCountToReturn As Integer, ByRef strMostRecentJobInfo As String) As String()

		' This regex will match all text up to the first comma (this is the time stamp), followed by a comma, then the error message, then the text ", Error,"
		Const ERROR_MATCH_REGEX As String = "^([^,]+),(.+), Error, *$"

		' This regex looks for information on a job starting
		Const JOB_START_REGEX As String = "^([^,]+),.+Started analysis job (\d+), Dataset (.+), Tool (.+), Normal"

		' The following effectively defines the number of days in the past to search when finding recent errors
		Const MAX_LOG_FILES_TO_SEARCH As Integer = 5

		Dim blnLoggerReportsError As Boolean
		Dim strLogFilePath As String
		Dim intLogFileCountProcessed As Integer

		Dim srInFile As StreamReader

		Dim reErrorLine As Regex
		Dim reJobStartLine As Regex

		Dim objMatch As Match

		Dim qErrorMsgQueue As Queue
		Dim htUniqueErrorMessages As Hashtable

		' Note that strRecentErrorMessages() and dtRecentErrorMessageDates() are parallel arrays
		Dim intRecentErrorMessageCount As Integer
		Dim strRecentErrorMessages() As String = New String() {}
		Dim dtRecentErrorMessageDates() As DateTime

		Dim strLineIn As String

		Dim blnCheckForMostRecentJob As Boolean
		Dim strMostRecentJobInfoFromLogs As String

		Dim strTimestamp As String
		Dim strErrorMessageClean As String

		Try
			If strMostRecentJobInfo Is Nothing Then strMostRecentJobInfo = String.Empty
			strMostRecentJobInfoFromLogs = String.Empty

			'If objLogger Is Nothing Then
			'    intRecentErrorMessageCount = 0
			'    ReDim strRecentErrorMessages(-1)
			'Else
			If intErrorMessageCountToReturn < 1 Then intErrorMessageCountToReturn = 1

			intRecentErrorMessageCount = 0
			ReDim strRecentErrorMessages(intErrorMessageCountToReturn - 1)
			ReDim dtRecentErrorMessageDates(strRecentErrorMessages.Length - 1)

			' Initialize the RegEx that splits out the timestamp from the error message
			reErrorLine = New Regex(ERROR_MATCH_REGEX, RegexOptions.Compiled Or RegexOptions.IgnoreCase)
			reJobStartLine = New Regex(JOB_START_REGEX, RegexOptions.Compiled Or RegexOptions.IgnoreCase)

			' Initialize the queue that holds recent error messages
			qErrorMsgQueue = New Queue(intErrorMessageCountToReturn)

			' Initialize the hashtable to hold the error messages, but without date stamps
			htUniqueErrorMessages = New Hashtable

			' Examine the most recent error reported by objLogger
			strLineIn = clsLogTools.MostRecentErrorMessage
			If Not strLineIn Is Nothing AndAlso strLineIn.Length > 0 Then
				blnLoggerReportsError = True
			Else
				blnLoggerReportsError = False
			End If


			strLogFilePath = GetRecentLogFilename()
			If intErrorMessageCountToReturn > 1 OrElse Not blnLoggerReportsError Then

				' Recent error message reported by objLogger is empty or intErrorMessageCountToReturn is greater than one
				' Open log file strLogFilePath to find the most recent error messages
				' If not enough error messages are found, we will look through previous log files

				intLogFileCountProcessed = 0
				blnCheckForMostRecentJob = True

				Do While qErrorMsgQueue.Count < intErrorMessageCountToReturn AndAlso intLogFileCountProcessed < MAX_LOG_FILES_TO_SEARCH

					If File.Exists(strLogFilePath) Then
						srInFile = New StreamReader(New FileStream(strLogFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))

						If intErrorMessageCountToReturn < 1 Then intErrorMessageCountToReturn = 1

						Do While srInFile.Peek >= 0
							strLineIn = srInFile.ReadLine

							If Not strLineIn Is Nothing Then
								objMatch = reErrorLine.Match(strLineIn)

								If objMatch.Success Then
									DetermineRecentErrorCacheError(objMatch, strLineIn, htUniqueErrorMessages, qErrorMsgQueue, intErrorMessageCountToReturn)
								End If

								If blnCheckForMostRecentJob Then
									objMatch = reJobStartLine.Match(strLineIn)
									If objMatch.Success Then
										Try
											strMostRecentJobInfoFromLogs = ConstructMostRecentJobInfoText(objMatch.Groups(1).Value, _
											  CInt(objMatch.Groups(2).Value), _
											  objMatch.Groups(3).Value, _
											  objMatch.Groups(4).Value)
										Catch ex As Exception
											' Ignore errors here
										End Try
									End If
								End If
							End If
						Loop

						srInFile.Close()

						If blnCheckForMostRecentJob AndAlso strMostRecentJobInfoFromLogs.Length > 0 Then
							' We determine the most recent job; no need to check other log files
							blnCheckForMostRecentJob = False
						End If

					Else
						' Log file not found; that's OK, we'll decrement the name by one day and keep checking
					End If

					' Increment the log file counter, regardless of whether or not the log file was found
					intLogFileCountProcessed += 1

					If qErrorMsgQueue.Count < intErrorMessageCountToReturn Then
						' We still haven't found intErrorMessageCountToReturn error messages
						' Keep checking older log files as long as qErrorMsgQueue.Count < intErrorMessageCountToReturn

						' Decrement the log file path by one day
						strLogFilePath = DecrementLogFilePath(strLogFilePath)
						If strLogFilePath Is Nothing OrElse strLogFilePath = String.Empty Then
							Exit Do
						End If
					End If
				Loop

			End If

			If blnLoggerReportsError Then
				' Append the error message reported by the Logger to the error message queue (treating it as the newest error)
				strLineIn = clsLogTools.MostRecentErrorMessage
				objMatch = reErrorLine.Match(strLineIn)

				If objMatch.Success Then
					DetermineRecentErrorCacheError(objMatch, strLineIn, htUniqueErrorMessages, qErrorMsgQueue, intErrorMessageCountToReturn)
				End If
			End If


			' Populate strRecentErrorMessages and dtRecentErrorMessageDates using the messages stored in qErrorMsgQueue
			Do While qErrorMsgQueue.Count > 0
				strErrorMessageClean = CStr(qErrorMsgQueue.Dequeue())

				' Find the newest timestamp for this message
				If htUniqueErrorMessages.ContainsKey(strErrorMessageClean) Then
					strTimestamp = CStr(htUniqueErrorMessages(strErrorMessageClean))
				Else
					' This code should not be reached
					strTimestamp = ""
				End If

				If intRecentErrorMessageCount >= strRecentErrorMessages.Length Then
					' Need to reserve more memory; this is unexpected
					ReDim Preserve strRecentErrorMessages(strRecentErrorMessages.Length * 2 - 1)
					ReDim Preserve dtRecentErrorMessageDates(strRecentErrorMessages.Length - 1)
				End If

				strRecentErrorMessages(intRecentErrorMessageCount) = strTimestamp & ", " & strErrorMessageClean.TrimStart(" "c)

				Try
					dtRecentErrorMessageDates(intRecentErrorMessageCount) = CDate(strTimestamp)
				Catch ex As Exception
					' Error converting date;
					dtRecentErrorMessageDates(intRecentErrorMessageCount) = DateTime.MinValue
				End Try

				intRecentErrorMessageCount += 1
			Loop

			If intRecentErrorMessageCount < strRecentErrorMessages.Length Then
				' Shrink the arrays
				ReDim Preserve strRecentErrorMessages(intRecentErrorMessageCount - 1)
				ReDim Preserve dtRecentErrorMessageDates(intRecentErrorMessageCount - 1)
			End If

			If intRecentErrorMessageCount > 1 Then
				' Sort the arrays by descending date
				Array.Sort(dtRecentErrorMessageDates, strRecentErrorMessages)
				Array.Reverse(dtRecentErrorMessageDates)
				Array.Reverse(strRecentErrorMessages)
			End If

			If strMostRecentJobInfo.Length = 0 Then
				If Not strMostRecentJobInfoFromLogs Is Nothing AndAlso strMostRecentJobInfoFromLogs.Length > 0 Then
					' Update strMostRecentJobInfo
					strMostRecentJobInfo = strMostRecentJobInfoFromLogs
				End If
			End If

		Catch ex As Exception
			' Ignore errors here
			Try
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in DetermineRecentErrorMessages", ex)
			Catch ex2 As Exception
				' Ignore errors logging the error
			End Try
		End Try

		Return strRecentErrorMessages

	End Function

	Protected Sub DetermineRecentErrorCacheError(ByRef objMatch As Match, _
	 ByVal strErrorMessage As String, _
	 ByRef htUniqueErrorMessages As Hashtable, _
	 ByRef qErrorMsgQueue As Queue, _
	 ByVal intMaxErrorMessageCountToReturn As Integer)

		Dim strTimestamp As String
		Dim strErrorMessageClean As String
		Dim strQueuedError As String

		Dim blnAddItemToQueue As Boolean
		Dim objItem As Object

		' See if this error is present in htUniqueErrorMessages yet
		' If it is present, update the timestamp in htUniqueErrorMessages
		' If not present, queue it

		If objMatch.Groups.Count >= 2 Then
			strTimestamp = objMatch.Groups(1).Value
			strErrorMessageClean = objMatch.Groups(2).Value
		Else
			' Regex didn't match; this is unexpected
			strTimestamp = DateTime.MinValue.ToString()
			strErrorMessageClean = strErrorMessage
		End If

		' Check whether strErrorMessageClean is in the hash table
		objItem = htUniqueErrorMessages.Item(strErrorMessageClean)
		If Not objItem Is Nothing Then
			' The error message is present
			' Update the timestamp associated with strErrorMessageClean if the time stamp is newer than the stored one
			Try
				If DateTime.Parse(strTimestamp) > DateTime.Parse(CStr(objItem)) Then
					htUniqueErrorMessages(strErrorMessageClean) = strTimestamp
				End If
			Catch ex As Exception
				' Date comparison failed; leave the existing timestamp unchanged
			End Try

		Else
			' The error message is not present
			htUniqueErrorMessages.Add(strErrorMessageClean, strTimestamp)
		End If

		If Not qErrorMsgQueue.Contains(strErrorMessageClean) Then
			' Queue this message
			' However, if we already have intErrorMessageCountToReturn messages queued, then dequeue the oldest one

			If qErrorMsgQueue.Count < intMaxErrorMessageCountToReturn Then
				qErrorMsgQueue.Enqueue(strErrorMessageClean)
			Else
				' Too many queued messages, so remove oldest one
				' However, only do this if the new error message has a timestamp newer than the oldest queued message
				'  (this is a consideration when processing multiple log files)

				blnAddItemToQueue = True

				strQueuedError = CStr(qErrorMsgQueue.Peek())

				' Get the timestamp associated with strQueuedError, as tracked by the hashtable
				objItem = htUniqueErrorMessages.Item(strQueuedError)
				If objItem Is Nothing Then
					' The error message is not in the hashtable; this is unexpected
				Else
					' Compare the queued error's timestamp with the timestamp of the new error message
					Try
						If DateTime.Parse(CStr(objItem)) >= DateTime.Parse(strTimestamp) Then
							' The queued error message's timestamp is equal to or newer than the new message's timestamp
							' Do not add the new item to the queue
							blnAddItemToQueue = False
						End If
					Catch ex As Exception
						' Date comparison failed; Do not add the new item to the queue
						blnAddItemToQueue = False
					End Try
				End If

				If blnAddItemToQueue Then
					qErrorMsgQueue.Dequeue()
					qErrorMsgQueue.Enqueue(strErrorMessageClean)
				End If

			End If
		End If

	End Sub


	''' <summary>
	''' Sets the local mgr_active flag to False for serious problems
	''' </summary>
	''' <remarks></remarks>
	Private Sub DisableManagerLocally()

		' Note: We previously called m_MgrSettings.DisableManagerLocally() to update AnalysisManager.config.exe
		' We now create a flag file instead
		' This gives the manager a chance to auto-cleanup things if ManagerErrorCleanupMode is >= 1

		m_MgrErrorCleanup.CreateStatusFlagFile()
		UpdateStatusFlagFileExists()

	End Sub

	''' <summary>
	''' Determines free disk space for the disk where the given directory resides.  Supports both fixed drive letters and UNC paths (e.g. \\Server\Share\)
	''' </summary>
	''' <param name="strDirectoryPath"></param>
	''' <param name="lngFreeBytesAvailableToUser"></param>
	''' <param name="lngTotalDriveCapacityBytes"></param>
	''' <param name="lngTotalNumberOfFreeBytes"></param>
	''' <returns>True if success, false if a problem</returns>
	''' <remarks></remarks>
	Private Function GetDiskFreeSpace(ByVal strDirectoryPath As String, ByRef lngFreeBytesAvailableToUser As Long, ByRef lngTotalDriveCapacityBytes As Long, ByRef lngTotalNumberOfFreeBytes As Long) As Boolean

		Dim intResult As Integer

		intResult = GetDiskFreeSpaceEx(strDirectoryPath, lngFreeBytesAvailableToUser, lngTotalDriveCapacityBytes, lngTotalNumberOfFreeBytes)

		If intResult = 0 Then
			Return False
		Else
			Return True
		End If

	End Function

	Protected Function GetRecentLogFilename() As String
		Dim lastFilename As String
		Dim x As Integer
		Dim Files() As String

		Try
			' Obtain a list of log files
			Files = Directory.GetFiles(m_MgrFolderPath, m_MgrSettings.GetParam("logfilename") & "*.txt")

			' Change the file names to lowercase (to assure that the sorting works)
			For x = 0 To Files.Length - 1
				Files(x) = Files(x).ToLower
			Next

			' Sort the files by filename
			Array.Sort(Files)

			' Return the last filename in the list
			lastFilename = Files(Files.Length - 1)

		Catch ex As Exception
			Return String.Empty
		End Try

		Return lastFilename
	End Function

	Protected Function GetManagerErrorCleanupMode() As clsCleanupMgrErrors.eCleanupModeConstants
		Dim strManagerErrorCleanupMode As String
		Dim eManagerErrorCleanupMode As clsCleanupMgrErrors.eCleanupModeConstants

		strManagerErrorCleanupMode = m_MgrSettings.GetParam("ManagerErrorCleanupMode")

		Select Case strManagerErrorCleanupMode.Trim
			Case "0"
				eManagerErrorCleanupMode = clsCleanupMgrErrors.eCleanupModeConstants.Disabled
			Case "1"
				eManagerErrorCleanupMode = clsCleanupMgrErrors.eCleanupModeConstants.CleanupOnce
			Case "2"
				eManagerErrorCleanupMode = clsCleanupMgrErrors.eCleanupModeConstants.CleanupAlways
			Case Else
				eManagerErrorCleanupMode = clsCleanupMgrErrors.eCleanupModeConstants.Disabled
		End Select

		Return eManagerErrorCleanupMode

	End Function

	''' <summary>
	''' Initializes the status file writing tool
	''' </summary>
	''' <remarks></remarks>
	Private Sub InitStatusTools()

		If m_StatusTools Is Nothing Then
			Dim StatusFileLoc As String = Path.Combine(m_MgrFolderPath, m_MgrSettings.GetParam("statusfilelocation"))

			If Me.TraceMode Then ShowTraceMessage("Initialize m_StatusTools using " & StatusFileLoc)
			m_StatusTools = New clsStatusFile(StatusFileLoc, m_DebugLevel)

			With m_StatusTools
				.TaskStartTime = DateTime.UtcNow
				.Dataset = ""
				.JobNumber = 0
				.JobStep = 0
				.Tool = ""
				.MgrName = m_MgrName
				.MgrStatus = IStatusFile.EnumMgrStatus.RUNNING
				.TaskStatus = IStatusFile.EnumTaskStatus.NO_TASK
				.TaskStatusDetail = IStatusFile.EnumTaskStatusDetail.NO_TASK
			End With

			UpdateStatusToolLoggingSettings(m_StatusTools)

		End If

	End Sub

	''' <summary>
	''' Loads the initial settings from application config file
	''' </summary>
	''' <returns>String dictionary containing initial settings if suceessful; NOTHING on error</returns>
	''' <remarks></remarks>
	Friend Shared Function LoadMgrSettingsFromFile() As Dictionary(Of String, String)

		'Load initial settings into string dictionary for return
		Dim lstMgrSettings As New Dictionary(Of String, String)(StringComparer.CurrentCultureIgnoreCase)

		' Note: When you are editing this project using the Visual Studio IDE, if you edit the values
		'  ->My Project>Settings.settings, then when you run the program (from within the IDE), then it
		'  will update file AnalysisManagerProg.exe.config with your settings
		' The manager will exit if the "UsingDefaults" value is "True", thus you need to have 
		'  "UsingDefaults" be "False" to run (and/or debug) the application

		My.Settings.Reload()

		'Manager config db connection string
		lstMgrSettings.Add("MgrCnfgDbConnectStr", My.Settings.MgrCnfgDbConnectStr)

		'Manager active flag
		lstMgrSettings.Add("MgrActive_Local", My.Settings.MgrActive_Local.ToString)

		'Manager name


		' Note: if the MgrName setting in the AnalysisManagerProg.exe.config file contains the text $ComputerName$
		'   then that text is replaced with this computer's domain name
		' This is a case-sensitive comparison

		lstMgrSettings.Add("MgrName", My.Settings.MgrName.Replace("$ComputerName$", Environment.MachineName))

		'Default settings in use flag
		lstMgrSettings.Add("UsingDefaults", My.Settings.UsingDefaults.ToString)

		Return lstMgrSettings

	End Function


	Private Function NeedToAbortProcessing() As Boolean

		If m_NeedToAbortProcessing Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Analysis manager has encountered a fatal error - aborting processing")
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "===== Closing Analysis Manager =====")
			Return True
		End If

		If Not m_StatusTools Is Nothing Then
			If m_StatusTools.AbortProcessingNow Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Found file " & clsStatusFile.ABORT_PROCESSING_NOW_FILENAME & " - aborting processing")
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "===== Closing Analysis Manager =====")
				Return True
			End If
		End If

		Return False
	End Function

	Private Sub PostToEventLog(ByVal ErrMsg As String)
		Const EVENT_LOG_NAME As String = "DMSAnalysisManager"

		Try
			Console.WriteLine()
			Console.WriteLine("===============================================================")
			Console.WriteLine(ErrMsg)
			Console.WriteLine("===============================================================")
			Console.WriteLine()
			Console.WriteLine("You may need to start this application once from an elevated (administrative level) command prompt using the /EL switch so that it can create the " & EVENT_LOG_NAME & " application log")
			Console.WriteLine()

			Dim Ev As New EventLog("Application", ".", EVENT_LOG_NAME)
			Trace.Listeners.Add(New EventLogTraceListener(EVENT_LOG_NAME))
			Trace.WriteLine(ErrMsg)
			Ev.Close()

		Catch ex As Exception
			Console.WriteLine()
			Console.WriteLine("Exception logging to the event log: " & ex.Message)
		End Try

		Thread.Sleep(500)

	End Sub


	Private Function ReloadManagerSettings() As Boolean

		Try
			If Me.TraceMode Then ShowTraceMessage("Reading application config file")

			'Get settings from config file
			Dim lstMgrSettings As Dictionary(Of String, String)
			lstMgrSettings = LoadMgrSettingsFromFile()

			If Me.TraceMode Then ShowTraceMessage("Storing manager settings in m_MgrSettings")
			If Not m_MgrSettings.LoadSettings(lstMgrSettings) Then
				If m_MgrSettings.ErrMsg <> "" Then
					'Manager has been deactivated, so report this
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_MgrSettings.ErrMsg)
					UpdateStatusDisabled(IStatusFile.EnumMgrStatus.DISABLED_LOCAL, "Disabled Locally")
				Else
					'Unknown problem reading config file
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Error re-reading config file")
				End If
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "===== Closing Analysis Manager =====")
				Return False
			End If

		Catch ex As Exception
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error re-loading manager settings: " & ex.Message)
			Return False
		End Try

		Return True


	End Function

	Private Sub RemoveTempFiles()

		Dim diMgrFolder As DirectoryInfo = New DirectoryInfo(m_MgrFolderPath)
		Dim msg As String

		' Files starting with the name IgnoreMe are created by log4NET when it is first instantiated 
		' This name is defined in the RollingFileAppender section of the Logging.config file via this XML:
		' <file value="IgnoreMe" />

		For Each fiFile As FileInfo In diMgrFolder.GetFiles("IgnoreMe*.txt")
			Try
				fiFile.Delete()
			Catch ex As Exception
				msg = "Error deleting IgnoreMe file: " & fiFile.Name
				Console.WriteLine(msg & " : " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex))
			End Try
		Next

		' Files named tmp.iso.#### and tmp.peak.#### (where #### are integers) are files created by Decon2LS
		' These files indicate a previous, failed Decon2LS task and can be safely deleted
		' For safety, we will not delete files less than 24 hours old

		Dim lstFilesToDelete As List(Of FileInfo) = diMgrFolder.GetFiles("tmp.iso.*").ToList()

		lstFilesToDelete.AddRange(diMgrFolder.GetFiles("tmp.peak.*"))

		For Each fiFile As FileInfo In lstFilesToDelete
			Try
				If DateTime.UtcNow.Subtract(fiFile.LastWriteTimeUtc).TotalHours > 24 Then
					If Me.TraceMode Then ShowTraceMessage("Deleting temp file " & fiFile.FullName)
					fiFile.Delete()
				End If
			Catch ex As Exception
				msg = "Error deleting file: " & fiFile.Name
				Console.WriteLine(msg & " : " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex))
			End Try
		Next


	End Sub

	Private Function SetResourceObject() As Boolean

		Dim strMessage As String
		Dim StepToolName As String = m_AnalysisTask.GetParam("StepTool")

		m_PluginLoader.ClearMessageList()
		m_Resource = m_PluginLoader.GetAnalysisResources(StepToolName.ToLower)
		If m_Resource Is Nothing Then
			Dim Msg As String = m_PluginLoader.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Unable to load resource object, " & Msg)
			Return False
		End If

		If m_DebugLevel > 0 Then
			strMessage = "Loaded resourcer for StepTool " & StepToolName
			If m_PluginLoader.Message.Length > 0 Then strMessage &= ": " & m_PluginLoader.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strMessage)
		End If

		m_Resource.Setup(m_MgrSettings, m_AnalysisTask)
		Return True

	End Function

	Private Function StatusFlagFileError() As Boolean

		Dim blnMgrCleanupSuccess As Boolean

		If m_MgrErrorCleanup.DetectStatusFlagFile() Then

			Try
				blnMgrCleanupSuccess = m_MgrErrorCleanup.AutoCleanupManagerErrors(GetManagerErrorCleanupMode(), m_DebugLevel)

			Catch ex As Exception

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error calling AutoCleanupManagerErrors, " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex))
				m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysis(): " & ex.Message, m_MostRecentJobInfo, True)

				blnMgrCleanupSuccess = False
			End Try

			If blnMgrCleanupSuccess Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Flag file found; automatically cleaned the work directory and deleted the flag file(s)")
				' No error; return false
				Return False
			Else
				' Error removing flag file; return true
				Return True
			End If

		End If

		' No error; return false
		Return False

	End Function

	Private Function SetToolRunnerObject() As Boolean
		Dim strMessage As String
		Dim StepToolName As String = m_AnalysisTask.GetParam("StepTool")

		m_PluginLoader.ClearMessageList()
		m_ToolRunner = m_PluginLoader.GetToolRunner(StepToolName.ToLower)
		If m_ToolRunner Is Nothing Then
			strMessage = "Unable to load tool runner for StepTool " & StepToolName & ": " & m_PluginLoader.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMessage)
			Return False
		End If

		If m_DebugLevel > 0 Then
			strMessage = "Loaded tool runner for StepTool " & m_AnalysisTask.GetCurrentJobToolDescription()
			If m_PluginLoader.Message.Length > 0 Then strMessage &= ": " & m_PluginLoader.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, strMessage)
		End If

		Try
			' Setup the new tool runner
			m_ToolRunner.Setup(m_MgrSettings, m_AnalysisTask, m_StatusTools, m_SummaryFile)
		Catch ex As Exception
			strMessage = "Exception calling ToolRunner.Setup(): " + ex.Message
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMessage)
			Return False
		End Try

		Return True

	End Function

	Public Shared Sub ShowTraceMessage(ByVal strMessage As String)
		Console.WriteLine(DateTime.Now.ToString("hh:mm:ss tt") & ": " & strMessage)
	End Sub

	Protected Sub UpdateClose(ByVal ManagerCloseMessage As String)
		Dim strErrorMessages() As String
		strErrorMessages = DetermineRecentErrorMessages(5, m_MostRecentJobInfo)

		m_StatusTools.UpdateClose(ManagerCloseMessage, strErrorMessages, m_MostRecentJobInfo, True)
	End Sub

	''' <summary>
	''' Reloads the manager settings from the manager control database 
	''' if at least MinutesBetweenUpdates minutes have elapsed since the last update
	''' </summary>
	''' <param name="dtLastConfigDBUpdate"></param>
	''' <param name="MinutesBetweenUpdates"></param>
	''' <returns></returns>
	''' <remarks></remarks>
	Protected Function UpdateManagerSettings(ByRef dtLastConfigDBUpdate As DateTime, ByVal MinutesBetweenUpdates As Double) As Boolean

		Dim blnSuccess As Boolean = True

		If (DateTime.UtcNow.Subtract(dtLastConfigDBUpdate).TotalMinutes >= MinutesBetweenUpdates) Then

			dtLastConfigDBUpdate = DateTime.UtcNow

			If Me.TraceMode Then ShowTraceMessage("Loading manager settings from the manager control DB")

			If Not m_MgrSettings.LoadDBSettings() Then
				Dim msg As String

				If (String.IsNullOrEmpty(m_MgrSettings.ErrMsg)) Then
					msg = "Error calling m_MgrSettings.LoadMgrSettingsFromDB to update manager settings"
				Else
					msg = m_MgrSettings.ErrMsg
				End If

				If Me.TraceMode Then ShowTraceMessage(msg)
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg)

				blnSuccess = False
			Else
				' Need to synchronize some of the settings
				UpdateStatusToolLoggingSettings(m_StatusTools)
			End If

		End If

		Return blnSuccess

	End Function

	Protected Sub UpdateStatusDisabled(ByVal ManagerStatus As IStatusFile.EnumMgrStatus, ByVal ManagerDisableMessage As String)
		Dim strErrorMessages() As String
		strErrorMessages = DetermineRecentErrorMessages(5, m_MostRecentJobInfo)
		m_StatusTools.UpdateDisabled(ManagerStatus, ManagerDisableMessage, strErrorMessages, m_MostRecentJobInfo)
	End Sub

	Protected Sub UpdateStatusFlagFileExists()
		Dim strErrorMessages() As String
		strErrorMessages = DetermineRecentErrorMessages(5, m_MostRecentJobInfo)
		m_StatusTools.UpdateFlagFileExists(strErrorMessages, m_MostRecentJobInfo)
	End Sub

	Protected Sub UpdateStatusIdle(ByVal ManagerIdleMessage As String)
		Dim strErrorMessages() As String
		strErrorMessages = DetermineRecentErrorMessages(5, m_MostRecentJobInfo)

		m_StatusTools.UpdateIdle(ManagerIdleMessage, strErrorMessages, m_MostRecentJobInfo, True)
	End Sub

	Private Sub UpdateStatusToolLoggingSettings(ByRef objStatusFile As clsStatusFile)


		Dim LogMemoryUsage As Boolean = m_MgrSettings.GetParam("LogMemoryUsage", False)
		Dim MinimumMemoryUsageLogInterval As Single = m_MgrSettings.GetParam("MinimumMemoryUsageLogInterval", 1)

		Dim LogStatusToBrokerDB As Boolean = m_MgrSettings.GetParam("LogStatusToBrokerDB", False)
		Dim BrokerDBConnectionString As String = m_MgrSettings.GetParam("brokerconnectionstring")
		Dim BrokerDBStatusUpdateIntervalMinutes As Single = m_MgrSettings.GetParam("BrokerDBStatusUpdateIntervalMinutes", 60)

		Dim LogStatusToMessageQueue As Boolean = m_MgrSettings.GetParam("LogStatusToMessageQueue", False)
		Dim MessageQueueURI As String = m_MgrSettings.GetParam("MessageQueueURI")
		Dim MessageQueueTopicMgrStatus As String = m_MgrSettings.GetParam("MessageQueueTopicMgrStatus")

		With objStatusFile
			.ConfigureMemoryLogging(LogMemoryUsage, MinimumMemoryUsageLogInterval, m_MgrFolderPath)
			.ConfigureBrokerDBLogging(LogStatusToBrokerDB, BrokerDBConnectionString, BrokerDBStatusUpdateIntervalMinutes)
			.ConfigureMessageQueueLogging(LogStatusToMessageQueue, MessageQueueURI, MessageQueueTopicMgrStatus, m_MgrName)
		End With

	End Sub

	''' <summary>
	''' Confirms that the drive with the working directory has sufficient free space
	''' Confirms that the remote share for storing results is accessible and has sufficient free space
	''' </summary>
	''' <param name="ErrorMessage"></param>
	''' <returns></returns>
	''' <remarks>Disables the manager if the working directory drive does not have enough space</remarks>
	Private Function ValidateFreeDiskSpace(ByRef ErrorMessage As String) As Boolean

		Const DEFAULT_DATASET_STORAGE_MIN_FREE_SPACE_GB As Integer = 10
		Const DEFAULT_TRANSFER_DIR_MIN_FREE_SPACE_GB As Integer = 10

		Const DEFAULT_WORKING_DIR_MIN_FREE_SPACE_MB As Integer = 750
		Const DEFAULT_ORG_DB_DIR_MIN_FREE_SPACE_MB As Integer = 750

		Dim DatasetStoragePath As String
		Dim DatasetStorageMinFreeSpaceGB As Integer
		Dim ioDatasetStoragePath As DirectoryInfo

		Dim WorkingDirMinFreeSpaceMB As Integer

		Dim TransferDir As String
		Dim TransferDirMinFreeSpaceGB As Integer

		Dim strStepToolNameLCase As String
		Dim OrgDbDir As String
		Dim OrgDbDirMinFreeSpaceMB As Integer

		ErrorMessage = String.Empty

		Try
			strStepToolNameLCase = m_AnalysisTask.GetParam("JobParameters", "StepTool").ToLower()

			If strStepToolNameLCase = "results_transfer" Then
				' We only need to evaluate the dataset storage folder for free space

				DatasetStoragePath = m_AnalysisTask.GetParam("DatasetStoragePath")
				DatasetStorageMinFreeSpaceGB = m_MgrSettings.GetParam("DatasetStorageMinFreeSpaceGB", DEFAULT_DATASET_STORAGE_MIN_FREE_SPACE_GB)

				If String.IsNullOrEmpty(DatasetStoragePath) Then
					ErrorMessage = "DatasetStoragePath job parameter is empty"
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ErrorMessage)
					Return False
				End If

				ioDatasetStoragePath = New DirectoryInfo(DatasetStoragePath)
				If Not ioDatasetStoragePath.Exists Then
					' Dataset folder not found; that's OK, since the Results Transfer plugin will auto-create it
					' Try to use the parent folder (or the parent of the parent)
					Do While Not ioDatasetStoragePath.Exists AndAlso Not ioDatasetStoragePath.Parent Is Nothing
						ioDatasetStoragePath = ioDatasetStoragePath.Parent
					Loop

					DatasetStoragePath = ioDatasetStoragePath.FullName
				End If

				If Not ValidateFreeDiskSpaceWork("Dataset directory", DatasetStoragePath, DatasetStorageMinFreeSpaceGB * 1024, ErrorMessage, clsLogTools.LoggerTypes.LogFile) Then
					Return False
				Else
					Return True
				End If
			End If

			WorkingDirMinFreeSpaceMB = m_MgrSettings.GetParam("WorkDirMinFreeSpaceMB", DEFAULT_WORKING_DIR_MIN_FREE_SPACE_MB)

			TransferDir = m_AnalysisTask.GetParam("JobParameters", "transferFolderPath")
			TransferDirMinFreeSpaceGB = m_MgrSettings.GetParam("TransferDirMinFreeSpaceGB", DEFAULT_TRANSFER_DIR_MIN_FREE_SPACE_GB)

			OrgDbDir = m_MgrSettings.GetParam("orgdbdir")
			OrgDbDirMinFreeSpaceMB = m_MgrSettings.GetParam("OrgDBDirMinFreeSpaceMB", DEFAULT_ORG_DB_DIR_MIN_FREE_SPACE_MB)

			' Verify that the working directory exists and that its drive has sufficient free space
			If Not ValidateFreeDiskSpaceWork("Working directory", m_WorkDirPath, WorkingDirMinFreeSpaceMB, ErrorMessage, clsLogTools.LoggerTypes.LogDb) Then
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Disabling manager since working directory problem")
				DisableManagerLocally()
				Return False
			End If

			' Verify that the remote transfer directory exists and that its drive has sufficient free space
			If Not ValidateFreeDiskSpaceWork("Transfer directory", TransferDir, TransferDirMinFreeSpaceGB * 1024, ErrorMessage, clsLogTools.LoggerTypes.LogFile) Then
				Return False
			End If

			' Possibly verify that the local fasta file cache directory hsa sufficient free space
			If strStepToolNameLCase.Contains("sequest") OrElse _
			   strStepToolNameLCase.Contains("xtandem") OrElse _
			   strStepToolNameLCase.Contains("inspect") OrElse _
			   strStepToolNameLCase.Contains("msgfdb") OrElse _
			   strStepToolNameLCase.Contains("msgfplus") OrElse _
			   strStepToolNameLCase.Contains("msalign") OrElse _
			   strStepToolNameLCase.Contains("omssa") Then

				If Not ValidateFreeDiskSpaceWork("Organism DB directory", OrgDbDir, OrgDbDirMinFreeSpaceMB, ErrorMessage, clsLogTools.LoggerTypes.LogFile) Then
					DisableManagerLocally()
					Return False
				End If

			End If

		Catch ex As Exception
			ErrorMessage = "Exception validating free space"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception validating free space", ex)
			Return False
		End Try

		Return True

	End Function

	Private Function ValidateFreeDiskSpaceWork(ByVal strDirectoryDescription As String, ByVal strDirectoryPath As String, ByVal intMinFreeSpaceMB As Integer, ByRef ErrorMessage As String, eLogLocationIfNotFound As clsLogTools.LoggerTypes) As Boolean

		Dim diDirectory As DirectoryInfo
		Dim diDrive As DriveInfo
		Dim dblFreeSpaceMB As Double

		diDirectory = New DirectoryInfo(strDirectoryPath)
		If Not diDirectory.Exists Then
			ErrorMessage = strDirectoryDescription & " not found: " & strDirectoryPath
			clsLogTools.WriteLog(eLogLocationIfNotFound, clsLogTools.LogLevels.ERROR, ErrorMessage)
			Return False
		End If

		If diDirectory.Root.FullName.StartsWith("\\") OrElse Not diDirectory.Root.FullName.Contains(":") Then
			' Directory path is a remote share; use GetDiskFreeSpaceEx in Kernel32.dll
			Dim lngFreeBytesAvailableToUser As Long
			Dim lngTotalNumberOfBytes As Long
			Dim lngTotalNumberOfFreeBytes As Long

			If GetDiskFreeSpace(diDirectory.FullName, lngFreeBytesAvailableToUser, lngTotalNumberOfBytes, lngTotalNumberOfFreeBytes) Then
				dblFreeSpaceMB = lngTotalNumberOfFreeBytes / 1024.0 / 1024.0
			Else
				dblFreeSpaceMB = 0
			End If

		Else
			' Directory is a local drive; can query with .NET
			diDrive = New DriveInfo(diDirectory.Root.FullName)
			dblFreeSpaceMB = diDrive.TotalFreeSpace / 1024.0 / 1024.0
		End If


		If dblFreeSpaceMB < intMinFreeSpaceMB Then
			ErrorMessage = strDirectoryDescription & " drive has less than " & intMinFreeSpaceMB.ToString & " MB free: " & CInt(dblFreeSpaceMB).ToString() & " MB"
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, ErrorMessage)
			Return False
		Else
			Return True
		End If

	End Function

	Private Function ValidateWorkingDir() As Boolean

		'Verifies working directory is properly specified and is empty
		Dim MsgStr As String

		'Verify working directory is valid
		If Not Directory.Exists(m_WorkDirPath) Then
			MsgStr = "Invalid working directory: " & m_WorkDirPath
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, MsgStr)
			Return False
		End If

		'Verify the working directory is empty
		Dim TmpFilArray() As String = Directory.GetFiles(m_WorkDirPath)
		Dim TmpDirArray() As String = Directory.GetDirectories(m_WorkDirPath)

		If (TmpDirArray.Length = 0) And (TmpFilArray.Length = 1) Then
			' If the only file in the working directory is a JobParameters xml file,
			'  then try to delete it, since it's likely left over from a previous job that never actually started
			Dim strFileToCheck As String
			strFileToCheck = Path.GetFileName(TmpFilArray(0))

			If strFileToCheck.StartsWith(clsGlobal.XML_FILENAME_PREFIX) AndAlso _
			   strFileToCheck.EndsWith(clsGlobal.XML_FILENAME_EXTENSION) Then
				Try
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Working directory contains a stray JobParameters file, deleting it: " & TmpFilArray(0))

					File.Delete(TmpFilArray(0))

					' Wait 0.5 second and then refresh TmpFilArray
					Thread.Sleep(500)

					' Now obtain a new listing of files
					TmpFilArray = Directory.GetFiles(m_WorkDirPath)
				Catch ex As Exception
					' Deletion failed
				End Try
			End If
		End If

		If (TmpDirArray.Length > 0) Or (TmpFilArray.Length > 0) Then
			MsgStr = "Working directory not empty: " & m_WorkDirPath
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, MsgStr)
			Return False
		End If

		'No problems found
		Return True

	End Function

#End Region

	''' <summary>
	''' Event handler for file watcher
	''' </summary>
	''' <param name="sender"></param>
	''' <param name="e"></param>
	''' <remarks></remarks>
	Private Sub m_FileWatcher_Changed(ByVal sender As Object, ByVal e As FileSystemEventArgs) Handles m_FileWatcher.Changed

		m_FileWatcher.EnableRaisingEvents = False
		m_ConfigChanged = True

		If m_DebugLevel > 3 Then
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Config file changed")
		End If

	End Sub

End Class
