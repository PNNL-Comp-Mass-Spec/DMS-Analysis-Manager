Imports System.IO
Imports System.Object
Imports System.Activator
Imports System.Xml
Imports PRISM.Logging
Imports AnalysisManagerBase
Imports AnalysisManagerBase.clsGlobal
Imports System.Reflection

Imports System.Diagnostics

Public Class clsMainProcess

	'TODO: Make sure fix for 100+ s-folders is incorporated in FTICR processing

#Region "Member Variables"
	Private myMgrSettings As clsAnalysisMgrSettings
	Private myAnalysisJob As clsAnalysisJob
	Private myLogger As ILogger
	Private myResource As IAnalysisResources
	Private myToolRunner As IToolRunner
	Private myStatusTools As clsStatusFile
	Private m_MaxJobsAllowed As Short = 9999
	Private m_JobsPerformed As Short
	Private m_JobsFound As Boolean
	Private Shared m_StartupClass As clsMainProcess
	Private m_IniFileChanged As Boolean = False
	Private WithEvents m_FileWatcher As New FileSystemWatcher
	Private m_IniFileName As String = "AnalysisManager.xml"
	Private m_MgrActive As Boolean = True
	'Shared m_EventLog As EventLog
	Private m_DebugLevel As Integer = 0
#End Region

	Private Function GetIniFilePath(ByVal IniFileName As String) As String
		Dim fi As New FileInfo(Application.ExecutablePath)
		Return Path.Combine(fi.DirectoryName, IniFileName)
	End Function

	Private Function SetResourceObject() As Boolean

		myResource = clsPluginLoader.GetAnalysisResources(myAnalysisJob.GetParam("tool").ToLower)
		If myResource Is Nothing Then
			Dim m As String = clsPluginLoader.Message
			myLogger.PostEntry("Unable to load resource object, " & m, ILogger.logMsgType.logError, True)
			Return False
		End If
		If m_DebugLevel > 0 Then
			myLogger.PostEntry("Loaded resourcer " & clsPluginLoader.Message, ILogger.logMsgType.logDebug, True)
		End If
		myResource.Setup(myMgrSettings, myAnalysisJob, myLogger)
		Return True

	End Function

	Private Function SetToolRunnerObject(ByVal StartTime As Date) As Boolean

		Dim toolName As String = myAnalysisJob.AssignedTool
		Dim clustered As Boolean = CBool(myMgrSettings.GetParam("sequest", "cluster"))

		myToolRunner = clsPluginLoader.GetToolRunner(toolName, clustered)
		If myToolRunner Is Nothing Then
			Dim m As String = clsPluginLoader.Message
			myLogger.PostEntry("Unable to load tool runner, " & m, ILogger.logMsgType.logError, True)
			Return False
		End If
		If m_DebugLevel > 0 Then
			myLogger.PostEntry("Loaded tool runner " & clsPluginLoader.Message, ILogger.logMsgType.logDebug, True)
		End If
		myToolRunner.Setup(myMgrSettings, myAnalysisJob, myLogger, myStatusTools)
		Return True

	End Function

	Private Sub DoAnalysisJob()

		Dim result As IJobParams.CloseOutType
		Dim MachName As String
		Dim JobNum As String
		Dim Dataset As String

		Try
			' create the object that will manage the analysis job parameters
			'
			myAnalysisJob = New clsAnalysisJob(myMgrSettings, myLogger)

			' request a new job using mgr parameters
			'
			MachName = myMgrSettings.GetParam("programcontrol", "machname")
			myLogger.PostEntry("Retrieving job", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
			Application.DoEvents()
			Try
				If Not myAnalysisJob.RequestJob() Then
					' didn't find a job
					myLogger.PostEntry(MachName & ": No jobs available for retrieval", ILogger.logMsgType.logHealth, LOG_DATABASE)
					m_JobsFound = False
					myStatusTools.UpdateIdle()
					Exit Sub
				End If
			Catch Err As Exception
				myLogger.PostEntry("clsMainProcess.DoAnalysisJob(), requesting job, Error " & _
				 Err.Message, ILogger.logMsgType.logError, True)
				FailCount += 1
				Exit Sub
			End Try

			'Found a job
			m_JobsFound = True
			InitSummary()
			JobNum = myAnalysisJob.GetParam("jobNum")
			Dataset = myAnalysisJob.GetParam("datasetNum")
			With myStatusTools
				.StartTime = Now
				.DatasetName = Dataset
				.JobNumber = JobNum
				.UpdateAndWrite(IStatusFile.JobStatus.STATUS_STARTING, 0, 0)
			End With
			myLogger.PostEntry(MachName & ": Started analysis job " & JobNum & ", Dataset " & Dataset, _
			 ILogger.logMsgType.logNormal, LOG_DATABASE)

			' create the object that will manage getting the job resources
			'
			If Not SetResourceObject() Then
				myLogger.PostEntry(MachName & ": Unable to SetResourceObject, job " & JobNum & ", Dataset " & Dataset, _
				 ILogger.logMsgType.logError, LOG_DATABASE)
				Dim TempComment As String = AppendToComment(myAnalysisJob.GetParam("comment"), "Unable to set resource object")
				myAnalysisJob.CloseJob(IJobParams.CloseOutType.CLOSEOUT_FAILED, "", _
				 AppendToComment(myAnalysisJob.GetParam("comment"), "Unable to set resource object"))
				'FailCount += 1		'No longer needed because CloseJob increments counter
				Exit Sub
			End If

			' create the object that will manage the analysis tool
			'
			If Not SetToolRunnerObject(Now) Then
				myLogger.PostEntry(MachName & ": Unable to SetToolRunnerObject, job " & JobNum & ", Dataset " & Dataset, _
				 ILogger.logMsgType.logError, LOG_DATABASE)
				myAnalysisJob.CloseJob(IJobParams.CloseOutType.CLOSEOUT_FAILED, "", _
				 AppendToComment(myAnalysisJob.GetParam("comment"), "Unable to set tool runner object"))
				'FailCount += 1		'No longer needed because CloseJob increments counter
				Exit Sub
			End If

			' create the object that will manage the analysis results
			'
			Dim myResults As New clsAnalysisResults(myMgrSettings, myAnalysisJob, myLogger)

			' Get the files required for running the job
			'
			myStatusTools.UpdateAndWrite(IStatusFile.JobStatus.STATUS_RETRIEVING_DATASET, 0, 0)
			CreateStatusFlagFile()			 'Set status file for control of future runs
			Try
				result = myResource.GetResources()
				If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
					myLogger.PostEntry(MachName & ": " & myResource.Message & ", Job " & JobNum & ", Dataset " & Dataset, _
					 ILogger.logMsgType.logError, LOG_DATABASE)
					myAnalysisJob.CloseJob(IJobParams.CloseOutType.CLOSEOUT_FAILED, "", _
					 AppendToComment(myAnalysisJob.GetParam("comment"), myResource.Message))
					If CleanWorkDir(myMgrSettings.GetParam("commonfileandfolderlocations", "workdir"), myLogger) Then
						DeleteStatusFlagFile(myLogger)
					End If
					myStatusTools.UpdateIdle()
					myLogger.PostEntry("===== Closing Analysis Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
					Exit Sub
				End If
			Catch Err As Exception
				FailCount += 1
				myLogger.PostEntry("clsMainProcess.DoAnalysisJob(), Getting resources," & _
				 Err.Message, ILogger.logMsgType.logError, True)
				myStatusTools.UpdateIdle()
				myLogger.PostEntry("===== Closing Analysis Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
				Exit Sub
			End Try

			' Run the job
			'
			myStatusTools.UpdateAndWrite(IStatusFile.JobStatus.STATUS_RUNNING, 0, 0)
			Try
				result = myToolRunner.RunTool
				If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
					myLogger.PostEntry(MachName & ": " & myToolRunner.Message & ", Job " & JobNum & ", Dataset " & Dataset, _
					 ILogger.logMsgType.logError, LOG_DATABASE)
					myAnalysisJob.CloseJob(result, "", AppendToComment(myAnalysisJob.GetParam("comment"), myToolRunner.Message))
					Try
						If CleanWorkDir(myMgrSettings.GetParam("commonfileandfolderlocations", "workdir"), myLogger) Then
							DeleteStatusFlagFile(myLogger)
						End If
						Exit Sub
					Catch Err As Exception
						FailCount += 1
						myLogger.PostEntry("clsMainProcess.DoAnalysisJob(), cleaning up after RunTool error," & _
						 Err.Message, ILogger.logMsgType.logError, True)
						myStatusTools.UpdateIdle()
						myLogger.PostEntry("===== Closing Analysis Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
						Exit Sub
					End Try
				End If
			Catch Err As Exception
				FailCount += 1
				myLogger.PostEntry("clsMainProcess.DoAnalysisJob(), running tool, " & Err.Message, _
				 ILogger.logMsgType.logError, True)
				myStatusTools.UpdateIdle()
				myLogger.PostEntry("===== Closing Analysis Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
				Exit Sub
			End Try

			myStatusTools.UpdateAndWrite(IStatusFile.JobStatus.STATUS_CLOSING, 0, 0)
			Try
				result = myResults.DeliverResults(myToolRunner.ResFolderName)
				If result <> IJobParams.CloseOutType.CLOSEOUT_SUCCESS Then
					myAnalysisJob.CloseJob(result, "", AppendToComment(myAnalysisJob.GetParam("comment"), myResults.Message))
					Try
						If CleanWorkDir(myMgrSettings.GetParam("commonfileandfolderlocations", "workdir"), myLogger) Then
							DeleteStatusFlagFile(myLogger)
						End If
						Exit Sub
					Catch Err As Exception
						FailCount += 1
						myLogger.PostEntry("clsMainProcess.DoAnalysisJob(), Cleaning up after DeliverResults error," & _
						 Err.Message, ILogger.logMsgType.logError, True)
						myStatusTools.UpdateIdle()
						myLogger.PostEntry("===== Closing Analysis Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
						Exit Sub
					End Try
				End If
			Catch Err As Exception
				myLogger.PostEntry("clsMainProcess.DoAnalysisJob(), Delivering results," & Err.Message, ILogger.logMsgType.logError, True)
				Exit Sub
			End Try

			'Clean the working directory
			Try
				If Not CleanWorkDir(myMgrSettings.GetParam("commonfileandfolderlocations", "workdir"), myLogger) Then
					myLogger.PostEntry("Error cleaning working directory, job " & myAnalysisJob.GetParam("jobNum"), _
					 ILogger.logMsgType.logError, LOG_DATABASE)
					myAnalysisJob.CloseJob(IJobParams.CloseOutType.CLOSEOUT_FAILED, "", _
					 AppendToComment(myAnalysisJob.GetParam("comment"), "Error cleaning working directory"))
					Exit Sub
				End If
			Catch Err As Exception
				FailCount += 1
				myLogger.PostEntry("clsMainProcess.DoAnalysisJob(), Clean work directory after normal run," & _
				 Err.Message, ILogger.logMsgType.logError, True)
				myStatusTools.UpdateIdle()
				myLogger.PostEntry("===== Closing Analysis Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
				Exit Sub
			End Try

			' If we got to here, then closeout the job as a success
			'
			DeleteStatusFlagFile(myLogger)
			FailCount = 0
			myAnalysisJob.CloseJob(IJobParams.CloseOutType.CLOSEOUT_SUCCESS, myToolRunner.ResFolderName, myAnalysisJob.GetParam("comment"))
			myLogger.PostEntry(MachName & ": Completed job " & JobNum, ILogger.logMsgType.logNormal, LOG_DATABASE)
			myStatusTools.UpdateIdle()
		Catch Err As Exception
			FailCount += 1
			myLogger.PostEntry("clsMainProcess.DoAnalysisJob(), " & Err.Message, ILogger.logMsgType.logError, True)
			myStatusTools.UpdateIdle()
			myLogger.PostEntry("===== Closing Analysis Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
			Exit Sub
		End Try
		'		OutputSummary()
	End Sub

	Public Sub DoAnalysis()

		Try
			m_JobsFound = True
			m_JobsPerformed = 0
			m_MaxJobsAllowed = CShort(myMgrSettings.GetParam("programcontrol", "maxjobcount"))
			m_DebugLevel = CInt(myMgrSettings.GetParam("programcontrol", "debuglevel"))

			While (m_JobsPerformed < m_MaxJobsAllowed) And m_JobsFound
				'Verify an error hasn't left the the system in an odd state
				If DetectStatusFlagFile() Then
					myLogger.PostEntry("Flag file exists - unable to perform any further analysis jobs", _
					 ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
					myStatusTools.UpdateFlagFileExists()
					myLogger.PostEntry("===== Closing Analysis Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
					Exit Sub
				End If
				'Check to see if the machine settings have changed
				If m_IniFileChanged Then
					m_IniFileChanged = False
					If Not ReReadIniFile() Then
						myLogger.PostEntry("Error re-reading ini file", ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
						Try
							myStatusTools.UpdateIdle()
						Catch
							'Do nothing - try/catch was used in case status file entries were wrong after re-read
						End Try
						myLogger.PostEntry("===== Closing Analysis Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
						Exit Sub
					End If
					m_FileWatcher.EnableRaisingEvents = True
				End If
				'Verify working directory properly specified and empty
				If Not ValidateWorkingDir() Then
					'Working directory problem, so exit
					myLogger.PostEntry("Working directory problem, disabling manager", ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
					m_MgrActive = False
					myMgrSettings.SetParam("programcontrol", "mgractive", m_MgrActive.ToString)
					myMgrSettings.SaveSettings()
				End If
				'Check to see if excessive consecutive failures have occurred
				If FailCount > 4 Then
					'More than 5 consecutive failures; there must be a generic problem, so exit
					myLogger.PostEntry("Multiple job failures, disabling manager", ILogger.logMsgType.logError, LOG_LOCAL_ONLY)
					m_MgrActive = False
					myMgrSettings.SetParam("programcontrol", "mgractive", m_MgrActive.ToString)
					myMgrSettings.SaveSettings()
				End If
				'Check to see if the manager is still active
				If Not m_MgrActive Then
					myLogger.PostEntry("Manager inactive", ILogger.logMsgType.logNormal, True)
					myStatusTools.UpdateDisabled()
					myLogger.PostEntry("===== Closing Analysis Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
					Exit Sub
				End If
				'Check to see if there are any analysis jobs ready
				DoAnalysisJob()
				m_JobsPerformed += 1S
			End While
			myStatusTools.UpdateIdle()
			myLogger.PostEntry("===== Closing Analysis Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
		Catch Err As Exception
			myLogger.PostEntry("clsMainProcess.DoAnalysis(), " & Err.Message, ILogger.logMsgType.logError, True)
			myStatusTools.UpdateIdle()
			myLogger.PostEntry("===== Closing Analysis Manager =====", ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
			Exit Sub
		End Try

	End Sub

	Shared Sub Main()

		Dim ErrMsg As String

		Try
			clsGlobal.AppFilePath = Application.ExecutablePath
			If IsNothing(m_StartupClass) Then
				m_StartupClass = New clsMainProcess
			End If
			m_StartupClass.DoAnalysis()
		Catch Err As Exception
			'Report any exceptions not handled at a lower level to the system application log
			ErrMsg = "Critical exception starting application: " & Err.Message
			Dim Ev As New EventLog("Application", ".", "DMSAnalysisManager")
			Trace.Listeners.Add(New EventLogTraceListener("DMSAnalysisManager"))
			Trace.WriteLine(ErrMsg)
			Ev.Close()
			Exit Sub
		End Try

	End Sub

	Public Sub New()

		Dim LogFile As String
		Dim ConnectStr As String
		Dim ModName As String
		Dim Fi As FileInfo

		Try
			'Load initial settings
			myMgrSettings = New clsAnalysisMgrSettings(GetIniFilePath(m_IniFileName))
			myStatusTools = New clsStatusFile(myMgrSettings.GetParam("commonfileandfolderlocations", "statusfilelocation"), _
			 myMgrSettings.GetParam("programcontrol", "maxduration"))
			myStatusTools.MachName = myMgrSettings.GetParam("programcontrol", "machname")
			m_MgrActive = CBool(myMgrSettings.GetParam("programcontrol", "mgractive"))

			' create the object that will manage the logging
			LogFile = myMgrSettings.GetParam("logging", "logfilename")
			ConnectStr = myMgrSettings.GetParam("databasesettings", "connectionstring")
			ModName = myMgrSettings.GetParam("programcontrol", "modulename")
			myLogger = New clsQueLogger(New clsDBLogger(ModName, ConnectStr, LogFile))

			'Write the initial log and status entries
			myLogger.PostEntry("===== Started Analysis Manager V" & Application.ProductVersion & " =====", _
			 ILogger.logMsgType.logNormal, LOG_LOCAL_ONLY)
			myStatusTools.WriteStatusFile()
		Catch Err As Exception
			'			m_EventLog.WriteEntry("clsMainProcess.New(), " & Err.Message)
			Throw New Exception("clsMainProcess.New(), " & Err.Message)
			Exit Sub
		End Try

		'Set up the FileWatcher to detect setup file changes
		Fi = New FileInfo(Application.ExecutablePath)
		m_FileWatcher.BeginInit()
		m_FileWatcher.Path = Fi.DirectoryName
		m_FileWatcher.IncludeSubdirectories = False
		m_FileWatcher.Filter = m_IniFileName
		m_FileWatcher.NotifyFilter = NotifyFilters.LastWrite Or NotifyFilters.Size
		m_FileWatcher.EndInit()
		m_FileWatcher.EnableRaisingEvents = True

	End Sub

	Private Sub m_FileWatcher_Changed(ByVal sender As Object, ByVal e As System.IO.FileSystemEventArgs) Handles m_FileWatcher.Changed
		m_IniFileChanged = True
		m_FileWatcher.EnableRaisingEvents = False		'Turn off change detection until current change has been acted upon
	End Sub

	Private Function ReReadIniFile() As Boolean

		'Re-read the ini file that may have changed
		'Note: Assumes log file and module name entries in ini file don't get changed

		If Not myMgrSettings.LoadSettings Then
			myLogger.PostEntry("Error reloading settings file", ILogger.logMsgType.logError, True)
			Return False
		End If

		'NOTE: Debug level has implied update because new instances of resource retrieval and analysis tool
		'	classes retrieve the debug level from MyMgrSettings in the constructor

		myStatusTools.FileLocation = myMgrSettings.GetParam("commonfileandfolderlocations", "statusfilelocation")
		myStatusTools.MachName = myMgrSettings.GetParam("programcontrol", "machname")
		myStatusTools.MaxJobDuration = myMgrSettings.GetParam("programcontrol", "maxduration")

		m_MaxJobsAllowed = CShort(myMgrSettings.GetParam("programcontrol", "maxjobcount"))
		m_MgrActive = CBool(myMgrSettings.GetParam("programcontrol", "mgractive"))
		'NOTE: Debug level implied update because new instances of resource retrieval and analysis tool
		'	classes retrieve the debug level from MyMgrSettings in the constructor
		Return True

	End Function

	Private Sub InitSummary()
		clsSummaryFile.Clear()
	End Sub


	Private Function ValidateWorkingDir() As Boolean

		'Verifies working directory is properly specified and is empty
		Dim WorkingDir As String = myMgrSettings.GetParam("commonfileandfolderlocations", "WorkDir")
		Dim MsgStr As String

		'Verify working directory is valid
		If Not Directory.Exists(WorkingDir) Then
			MsgStr = "Invalid working directory"
			myLogger.PostEntry(MsgStr, ILogger.logMsgType.logError, LOG_DATABASE)
			Return False
		End If

		'Verify the working directory is empty
		Dim TmpDirArray() As String = Directory.GetFiles(WorkingDir)
		Dim TmpFilArray() As String = Directory.GetDirectories(WorkingDir)
		If (TmpDirArray.GetLength(0) > 0) Or (TmpFilArray.GetLength(0) > 0) Then
			MsgStr = "Working directory not empty"
			myLogger.PostEntry(MsgStr, ILogger.logMsgType.logError, LOG_DATABASE)
			Return False
		End If

		'No problems found
		Return True

	End Function

End Class
