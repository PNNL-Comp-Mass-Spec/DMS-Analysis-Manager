'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/19/2007
'
' Last modified 01/16/2008
'*********************************************************************************************************

Imports System.IO
Imports PRISM.Logging
Imports AnalysisManagerBase
Imports AnalysisManagerBase.clsGlobal

Namespace AnalysisManagerProg

	Public Class clsMainProcess

		'*********************************************************************************************************
		'Master processing class for analysis manager
		'*********************************************************************************************************

#Region "Constants"
		Private Const CUSTOM_LOG_SOURCE_NAME As String = "Analysis Manager"
		Private Const CUSTOM_LOG_NAME As String = "DMS_AnalysisMgr"
		Private Const MAX_ERROR_COUNT As Integer = 4
#End Region

#Region "Module variables"
		Private Shared m_MainProcess As clsMainProcess
		Private m_MgrSettings As clsAnalysisMgrSettings
		Private m_AnalysisTask As clsAnalysisJob
		Private m_Logger As ILogger
		Private WithEvents m_FileWatcher As FileSystemWatcher
		Private m_ConfigChanged As Boolean = False
		Private m_OneTaskPerformed As Boolean = False
		Private m_TaskFound As Boolean = False
		Private m_DebugLevel As Integer = 0
		Private m_ErrorCount As Integer = 0
		Private m_FirstRun As Boolean = True
		Private m_Resource As IAnalysisResources
		Private m_ToolRunner As IToolRunner
		Private m_StatusTools As clsStatusFile
#End Region

#Region "Properties"
#End Region

#Region "Methods"
		''' <summary>
		''' Starts program execution
		''' </summary>
		''' <remarks></remarks>
		Shared Sub Main()

			Dim ErrMsg As String

			Try
				If IsNothing(m_MainProcess) Then
					m_MainProcess = New clsMainProcess
					If Not m_MainProcess.InitMgr Then Exit Sub
				End If
				clsGlobal.AppFilePath = Application.ExecutablePath
				m_MainProcess.DoAnalysis()
			Catch Err As System.Exception
				'Report any exceptions not handled at a lower level to the system application log
				ErrMsg = "Critical exception starting application: " & Err.Message
				Dim Ev As New EventLog("Application", ".", "DMSAnalysisManager")
				Trace.Listeners.Add(New EventLogTraceListener("DMSAnalysisManager"))
				Trace.WriteLine(ErrMsg)
				Ev.Close()
				Exit Sub
			End Try

		End Sub

		''' <summary>
		''' Constructor
		''' </summary>
		''' <remarks>Doesn't do anything at present</remarks>
		Public Sub New()

		End Sub

		''' <summary>
		''' Initializes the manager settings
		''' </summary>
		''' <returns>TRUE for success, FALSE for failure</returns>
		''' <remarks></remarks>
		Private Function InitMgr() As Boolean

			'Get the manager settings
			Try
				m_MgrSettings = New clsAnalysisMgrSettings(CUSTOM_LOG_SOURCE_NAME, CUSTOM_LOG_NAME)
			Catch ex As System.Exception
				'Failures are logged by clsMgrSettings to application event logs
				Return False
			End Try

			'Setup the logger
			Dim FInfo As FileInfo = New FileInfo(Application.ExecutablePath)
			Dim LogFileName As String = Path.Combine(FInfo.DirectoryName, m_MgrSettings.GetParam("logfilename"))
			Dim DbLogger As New clsDBLogger
			DbLogger.LogFilePath = LogFileName
			DbLogger.ConnectionString = m_MgrSettings.GetParam("connectionstring")
			DbLogger.ModuleName = m_MgrSettings.GetParam("modulename")
			m_Logger = New clsQueLogger(DbLogger)
			DbLogger = Nothing

			'Make the initial log entry
			Dim MyMsg As String = "=== Started Analysis Manager V" & Application.ProductVersion & " ===== "
			m_Logger.PostEntry(MyMsg, ILogger.logMsgType.logNormal, True)

			'Setup a file watcher for the config file
			m_FileWatcher = New FileSystemWatcher
			With m_FileWatcher
				.BeginInit()
				.Path = FInfo.DirectoryName
				.IncludeSubdirectories = False
				.Filter = m_MgrSettings.GetParam("configfilename")
				.NotifyFilter = NotifyFilters.LastWrite Or NotifyFilters.Size
				.EndInit()
				.EnableRaisingEvents = True
			End With

			'Get the debug level
			m_DebugLevel = CInt(m_MgrSettings.GetParam("debuglevel"))

			'Setup the tool for getting tasks
			m_AnalysisTask = New clsAnalysisJob(m_MgrSettings, m_Logger, m_DebugLevel)

			'Everything worked
			Return True

		End Function

		''' <summary>
		''' Loop to perform all analysis jobs
		''' </summary>
		''' <remarks></remarks>
		Public Sub DoAnalysis()

			Dim TaskCount As Integer = 0
			Dim MaxTaskCount As Integer = CInt(m_MgrSettings.GetParam("maxrepetitions"))
			m_TaskFound = True
			m_OneTaskPerformed = False

			InitStatusTools()

			While (TaskCount < MaxTaskCount) And m_TaskFound

				'Verify an error hasn't left the the system in an odd state
				If DetectStatusFlagFile() Then
					m_StatusTools.UpdateIdle()
					m_Logger.PostEntry("Flag file exists - unable to perform any further analysis jobs", _
					  ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
					m_Logger.PostEntry("===== Closing Analysis Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
					Exit Sub
				End If

				'Check to see if the machine settings have changed
				If m_ConfigChanged Then
					'Local config file has changed
					m_ConfigChanged = False
					If Not m_MgrSettings.LoadSettings(m_Logger) Then
						If m_MgrSettings.ErrMsg <> "" Then
							'Manager has been deactivated, so report this
							m_StatusTools.UpdateDisabled()
							m_Logger.PostEntry(m_MgrSettings.ErrMsg, ILogger.logMsgType.logWarning, LOG_LOCAL_ONLY)
						Else
							'Unknown problem reading config file
							m_Logger.PostEntry("Error re-reading config file", ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
						End If
						m_Logger.PostEntry("===== Closing Analysis Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
						Exit Sub
					End If
					m_FileWatcher.EnableRaisingEvents = True
				Else
					If m_FirstRun Then
						'No need to check for mgr control db changes since they were just loaded
						m_FirstRun = False
					Else	'm_FirstRun check
						'Check if manager control database settings have changed
						If Not m_MgrSettings.LoadMgrSettingsFromDB(m_Logger) Then
							m_Logger.PostEntry(m_MgrSettings.ErrMsg, ILogger.logMsgType.logError, True)
							m_Logger.PostEntry("===== Closing Analysis Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
							Exit Sub
						End If
					End If	'm_FirstRun check
				End If

				'Check to see if manager is still active
				Dim MgrActive As Boolean = CBool(m_MgrSettings.GetParam("mgractive")) And CBool(m_MgrSettings.GetParam("mgractive_local"))
				If Not MgrActive Then
					m_StatusTools.UpdateDisabled()
					m_Logger.PostEntry("Manager inactive", ILogger.logMsgType.logNormal, True)
					m_Logger.PostEntry("===== Closing Analysis Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
					Exit Sub
				End If

				'Verify working directory properly specified and empty
				If Not ValidateWorkingDir() Then
					'Working directory problem, so exit
					m_Logger.PostEntry("Working directory problem, disabling manager", ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
					DisableManagerLocally()
					m_StatusTools.UpdateDisabled()
					Exit While
				End If

				'Check to see if an excessive number of errors have occurred
				If m_ErrorCount > MAX_ERROR_COUNT Then
					m_Logger.PostEntry("Excessive task failures; disabling manager", ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
					DisableManagerLocally()
					m_StatusTools.UpdateDisabled()
					Exit While
				End If

				'Get an analysis job, if any are available
				Dim TaskReturn As clsAnalysisJob.RequestTaskResult = m_AnalysisTask.RequestTask
				Select Case TaskReturn
					Case clsDBTask.RequestTaskResult.NoTaskFound
						'No tasks found
						m_Logger.PostEntry("No analysis jobs found", ILogger.logMsgType.logHealth, LOG_DATABASE)
						m_TaskFound = False
						m_ErrorCount = 0
						m_StatusTools.UpdateIdle()
					Case clsDBTask.RequestTaskResult.ResultError
						'There was a problem getting the task; errors were logged by RequestTaskResult
						m_ErrorCount += 1
					Case clsDBTask.RequestTaskResult.TaskFound
						m_TaskFound = True
						If DoAnalysisJob() Then
							m_ErrorCount = 0
							m_OneTaskPerformed = True
							m_AnalysisTask.CloseTask(True)
						Else
							'Something went wrong; errors were logged by DoAnalysisJob
							m_ErrorCount += 1
							m_AnalysisTask.CloseTask(False)
						End If
					Case Else
						'Shouldn't ever get here
						Dim MyErr As String = "clsMainProcess.DoAnalysis; Invalid request result: "
						MyErr &= CInt(TaskReturn).ToString
						m_Logger.PostEntry(MyErr, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
						Exit Sub
				End Select
				TaskCount += 1

			End While

			If m_OneTaskPerformed Then
				m_Logger.PostEntry("Analysis complete for all available jobs", ILogger.logMsgType.logNormal, LOG_DATABASE)
			End If

			m_Logger.PostEntry("===== Closing Analysis Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)

		End Sub

		''' <summary>
		''' Sets the local mgr_active flag to False for serious problems
		''' </summary>
		''' <remarks></remarks>
		Private Sub DisableManagerLocally()

			If Not m_MgrSettings.WriteConfigSetting("MgrActive_Local", "False") Then
				m_Logger.PostEntry("Error while disabling manager: " & m_MgrSettings.ErrMsg, ILogger.logMsgType.logError, True)
			End If

		End Sub

		''' <summary>
		''' Event handler for file watcher
		''' </summary>
		''' <param name="sender"></param>
		''' <param name="e"></param>
		''' <remarks></remarks>
		Private Sub m_FileWatcher_Changed(ByVal sender As Object, ByVal e As System.IO.FileSystemEventArgs) Handles m_FileWatcher.Changed

			m_FileWatcher.EnableRaisingEvents = False
			m_ConfigChanged = True

			If m_DebugLevel > 3 Then
				m_Logger.PostEntry("Config file changed", ILogger.logMsgType.logDebug, True)
			End If

		End Sub

		Private Function DoAnalysisJob() As Boolean

			Dim Result As IJobParams.CloseOutType
			Dim MgrName As String = m_MgrSettings.GetParam("MgrName")
			Dim JobNum As Integer = CInt(m_AnalysisTask.GetParam("JobNum"))
			Dim Dataset As String = m_AnalysisTask.GetParam("DatasetNum")

			Try
				'Initialize summary and status files
				InitSummary()
				If m_StatusTools Is Nothing Then
					Dim FInfo As FileInfo = New FileInfo(Application.ExecutablePath)
					Dim StatusFileLoc As String = Path.Combine(FInfo.DirectoryName, m_MgrSettings.GetParam("statusfilelocation"))
					m_StatusTools = New clsStatusFile(StatusFileLoc)
				End If
				With m_StatusTools
					.StartTime = Now
					.DatasetName = Dataset
					.JobNumber = JobNum.ToString
					.MachName = m_MgrSettings.GetParam("MgrName")
					.UpdateAndWrite(IStatusFile.JobStatus.STATUS_STARTING, 0, 0)
				End With

				m_Logger.PostEntry(MgrName & ": Started analysis job " & JobNum & ", Dataset " & Dataset, _
				  ILogger.logMsgType.logNormal, LOG_DATABASE)

				'Create an object to manage the job resources
				If Not SetResourceObject() Then
					m_Logger.PostEntry(MgrName & ": Unable to SetResourceObject, job " & JobNum & ", Dataset " & Dataset, _
					  ILogger.logMsgType.logError, LOG_DATABASE)
					Dim TempComment As String = AppendToComment(m_AnalysisTask.GetParam("comment"), "Unable to set resource object")
					m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "", _
					  AppendToComment(m_AnalysisTask.GetParam("comment"), "Unable to set resource object"))
					m_StatusTools.UpdateIdle()
					Return False
				End If

				'Create an object to run the analysis tool
				If Not SetToolRunnerObject(Now) Then
					m_Logger.PostEntry(MgrName & ": Unable to SetToolRunnerObject, job " & JobNum & ", Dataset " & Dataset, _
					  ILogger.logMsgType.logError, LOG_DATABASE)
					m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "", _
					 AppendToComment(m_AnalysisTask.GetParam("comment"), "Unable to set tool runner object"))
					m_StatusTools.UpdateIdle()
					Return False
				End If

				'Create the object that handles analysis results
				Dim myResults As New clsAnalysisResults(m_MgrSettings, m_AnalysisTask, m_Logger)

				'Retrieve files required for the job
				m_StatusTools.UpdateAndWrite(IStatusFile.JobStatus.STATUS_RETRIEVING_DATASET, 0, 0)
				CreateStatusFlagFile()
				Try
					Result = m_Resource.GetResources()
					If Result = IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
						m_ToolRunner.SetResourcerDataFileList(m_Resource.DataFileList)
					Else
						m_Logger.PostEntry(MgrName & ": " & m_Resource.Message & ", Job " & JobNum & ", Dataset " & Dataset, _
						  ILogger.logMsgType.logError, LOG_DATABASE)
						m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "", _
						 AppendToComment(m_AnalysisTask.GetParam("comment"), m_Resource.Message))
						If CleanWorkDir(m_MgrSettings.GetParam("workdir"), m_Logger) Then
							DeleteStatusFlagFile(m_Logger)
						End If
						m_StatusTools.UpdateIdle()
						clsGlobal.DeleteStatusFlagFile(m_Logger)
						Return False
					End If
				Catch Err As Exception
					m_Logger.PostEntry("clsMainProcess.DoAnalysisJob(), Getting resources," & _
					  Err.Message, ILogger.logMsgType.logError, True)
					m_StatusTools.UpdateIdle()
					Return False
				End Try

				'Run the job
				m_StatusTools.UpdateAndWrite(IStatusFile.JobStatus.STATUS_RUNNING, 0, 0)
				Try
					Result = m_ToolRunner.RunTool
					If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
						m_Logger.PostEntry(MgrName & ": " & m_ToolRunner.Message & ", Job " & JobNum & ", Dataset " & Dataset, _
						  ILogger.logMsgType.logError, LOG_DATABASE)
						m_AnalysisTask.CloseTask(Result, "", AppendToComment(m_AnalysisTask.GetParam("comment"), m_ToolRunner.Message))
						Try
							If CleanWorkDir(m_MgrSettings.GetParam("workdir"), m_Logger) Then
								DeleteStatusFlagFile(m_Logger)
							End If
							Return False
						Catch Err As Exception
							m_Logger.PostEntry("clsMainProcess.DoAnalysisJob(), cleaning up after RunTool error," & _
							  Err.Message, ILogger.logMsgType.logError, True)
							m_StatusTools.UpdateIdle()
							Return False
						End Try
					End If
				Catch Err As Exception
					m_Logger.PostEntry("clsMainProcess.DoAnalysisJob(), running tool, " & Err.Message, _
						ILogger.logMsgType.logError, True)
					m_StatusTools.UpdateIdle()
					Return False
				End Try

				'Close out the job
				m_StatusTools.UpdateAndWrite(IStatusFile.JobStatus.STATUS_CLOSING, 0, 0)
				Try
					Result = myResults.DeliverResults(m_ToolRunner.ResFolderName)
					If Result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
						m_AnalysisTask.CloseTask(Result, "", AppendToComment(m_AnalysisTask.GetParam("comment"), myResults.Message))
						Try
							If CleanWorkDir(m_MgrSettings.GetParam("workdir"), m_Logger) Then
								DeleteStatusFlagFile(m_Logger)
							End If
							Return False
						Catch Err As Exception
							m_Logger.PostEntry("clsMainProcess.DoAnalysisJob(), Cleaning up after DeliverResults error," & _
							  Err.Message, ILogger.logMsgType.logError, True)
							m_StatusTools.UpdateIdle()
							Return False
						End Try
					End If
				Catch Err As Exception
					m_Logger.PostEntry("clsMainProcess.DoAnalysisJob(), Delivering results," & Err.Message, ILogger.logMsgType.logError, True)
					Return False
				End Try

				'Clean the working directory
				Try
					If Not CleanWorkDir(m_MgrSettings.GetParam("workdir"), m_Logger) Then
						m_Logger.PostEntry("Error cleaning working directory, job " & m_AnalysisTask.GetParam("jobNum"), _
						  ILogger.logMsgType.logError, LOG_DATABASE)
						m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_FAILED, "", _
						 AppendToComment(m_AnalysisTask.GetParam("comment"), "Error cleaning working directory"))
						Return False
					End If
				Catch Err As Exception
					m_Logger.PostEntry("clsMainProcess.DoAnalysisJob(), Clean work directory after normal run," & _
					 Err.Message, ILogger.logMsgType.logError, True)
					m_StatusTools.UpdateIdle()
					Return False
				End Try

				'Close out the job as a success
				DeleteStatusFlagFile(m_Logger)
				m_AnalysisTask.CloseTask(IJobParams.CloseOutType.CLOSEOUT_SUCCESS, m_ToolRunner.ResFolderName, m_AnalysisTask.GetParam("comment"))
				m_Logger.PostEntry(MgrName & ": Completed job " & JobNum, ILogger.logMsgType.logNormal, LOG_DATABASE)
				m_StatusTools.UpdateIdle()
				Return True

			Catch ex As Exception
				m_Logger.PostEntry("clsMainProcess.DoAnalysisJob(), " & ex.Message, ILogger.logMsgType.logError, True)
				m_StatusTools.UpdateIdle()
				Return False
			End Try

		End Function

		Private Function SetResourceObject() As Boolean

			m_Resource = clsPluginLoader.GetAnalysisResources(m_AnalysisTask.GetParam("toolname").ToLower)
			If m_Resource Is Nothing Then
				Dim Msg As String = clsPluginLoader.Message
				m_Logger.PostEntry("Unable to load resource object, " & Msg, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
				Return False
			End If
			If m_DebugLevel > 0 Then
				m_Logger.PostEntry("Loaded resourcer " & clsPluginLoader.Message, ILogger.logMsgType.logDebug, LOG_LOCAL_ONLY)
			End If
			m_Resource.Setup(m_MgrSettings, m_AnalysisTask, m_Logger)
			Return True

		End Function

		Private Function SetToolRunnerObject(ByVal StartTime As Date) As Boolean

			Dim toolName As String = m_AnalysisTask.GetParam("toolname").ToLower
			'TODO: May be able to get rid of cluster parameter
			Dim clustered As Boolean = CBool(m_MgrSettings.GetParam("cluster"))

			m_ToolRunner = clsPluginLoader.GetToolRunner(toolName, clustered)
			If m_ToolRunner Is Nothing Then
				Dim m As String = clsPluginLoader.Message
				m_Logger.PostEntry("Unable to load tool runner, " & m, ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
				Return False
			End If
			If m_DebugLevel > 0 Then
				m_Logger.PostEntry("Loaded tool runner " & clsPluginLoader.Message, ILogger.logMsgType.logDebug, LOG_LOCAL_ONLY)
			End If
			m_ToolRunner.Setup(m_MgrSettings, m_AnalysisTask, m_Logger, m_StatusTools)
			Return True

		End Function

		Private Sub InitSummary()
			clsSummaryFile.Clear()
		End Sub

		Private Function ValidateWorkingDir() As Boolean

			'Verifies working directory is properly specified and is empty
			Dim WorkingDir As String = m_MgrSettings.GetParam("WorkDir")
			Dim MsgStr As String

			'Verify working directory is valid
			If Not Directory.Exists(WorkingDir) Then
				MsgStr = "Invalid working directory: " & WorkingDir
				m_Logger.PostEntry(MsgStr, ILogger.logMsgType.logError, LOG_DATABASE)
				Return False
			End If

			'Verify the working directory is empty
			Dim TmpDirArray() As String = Directory.GetFiles(WorkingDir)
			Dim TmpFilArray() As String = Directory.GetDirectories(WorkingDir)
			If (TmpDirArray.GetLength(0) > 0) Or (TmpFilArray.GetLength(0) > 0) Then
				MsgStr = "Working directory not empty"
				m_Logger.PostEntry(MsgStr, ILogger.logMsgType.logError, LOG_DATABASE)
				Return False
			End If

			'No problems found
			Return True

		End Function

		''' <summary>
		''' Initializes the status file writing tool
		''' </summary>
		''' <remarks></remarks>
		Private Sub InitStatusTools()

			If m_StatusTools Is Nothing Then
				Dim FInfo As FileInfo = New FileInfo(Application.ExecutablePath)
				Dim StatusFileLoc As String = Path.Combine(FInfo.DirectoryName, m_MgrSettings.GetParam("statusfilelocation"))
				m_StatusTools = New clsStatusFile(StatusFileLoc)
				With m_StatusTools
					.StartTime = Now
					.DatasetName = ""
					.JobNumber = ""
					.MachName = m_MgrSettings.GetParam("MgrName")
				End With
			End If

		End Sub
#End Region

	End Class

End Namespace
